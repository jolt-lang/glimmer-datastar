(ns jolt.datastar.tick
  "The render loop behind the SSE streams: one per wrap-datastar.

  Renders happen on a tick, not per change. A ratom changing marks the
  connections that read it dirty; at most once per tick-ms the loop renders
  every dirty connection, once, however many writes landed since the last
  tick. A page that a thousand writes touch in one tick renders once, not a
  thousand times, so the cost of a burst is bounded by the number of open
  pages rather than writes times pages. When nothing is dirty the loop parks
  and costs nothing.

  Writes can be batched too, as an opt-in: a thunk handed to tx! runs on the
  loop thread at the start of the next tick, before any render. Code that
  makes all its writes through tx! never has a render observe half of an
  update, since writing and rendering take turns; a :batch-fn sees each tick's
  thunks together (to run them in one database transaction, say).

  A tick's renders are spread over :render-threads workers. Each connection's
  frames go out through a sliding buffer of one, so a client that cannot keep
  up is sent the newest frame and skips the ones in between; nothing waits on
  it."
  (:require [clojure.core.async :as async]
            [glimmer.ratom :as ratom]
            [jolt.datastar.cache :as cache]
            [jolt.datastar.html :as html]))

(defn- now-ms [] (quot (System/nanoTime) 1000000))

(defn- run-batch!
  "Run one tick's tx! thunks. Each [thunk promise] pair gets its result (or
  the exception it threw) delivered."
  [batch-fn batch]
  (when (seq batch)
    (if batch-fn
      (try
        (batch-fn (mapv first batch))
        (doseq [[_ p] batch] (deliver p nil))
        (catch Throwable t
          (doseq [[_ p] batch] (deliver p t))))
      (doseq [[thunk p] batch]
        (deliver p (try (thunk) (catch Throwable t t)))))))

(defn- render-conn!
  "Render one connection and send the frame if the HTML changed."
  [{:keys [cache]} {:keys [render closed? prev-hash ch watcher]}]
  (when-not @closed?
    (try
      (let [frame (binding [ratom/*current-watcher* watcher
                            html/*cache* cache]
                    (render))
            h (if frame (hash frame) @prev-hash)]
        (when (not= h @prev-hash)
          (if (async/put! ch frame)
            (reset! prev-hash h)
            (reset! closed? true))))
      (catch Throwable t
        (println "jolt.datastar: render failed:" (ex-message t))))))

(defn- start-workers!
  "n render threads taking [conn done!] work items off a queue."
  [sched n]
  (let [q (java.util.concurrent.LinkedBlockingQueue.)]
    (dotimes [_ n]
      (async/thread
        (loop []
          (let [[conn done!] (.take q)]
            (when conn
              (render-conn! sched conn)
              (done!)
              (recur))))))
    q))

(defn- render-all!
  "Render conns, on the workers when there are any, and wait for all of them."
  [{:keys [work] :as sched} conns]
  (if (or (nil? @work) (< (count conns) 2))
    (run! #(render-conn! sched %) conns)
    (let [remaining (atom (count conns))
          finished  (promise)
          done!     #(when (zero? (swap! remaining dec)) (deliver finished true))]
      (doseq [c conns] (.put ^java.util.concurrent.LinkedBlockingQueue @work [c done!]))
      @finished)))

(defn- tick!
  [{:keys [txs dirty batch-fn] :as sched}]
  (let [batch (let [b (java.util.ArrayList.)]
                (loop []
                  (when-let [x (.poll ^java.util.concurrent.LinkedBlockingQueue txs)]
                    (.add b x)
                    (recur)))
                (vec b))]
    (run-batch! batch-fn batch)
    ;; snapshot after the writes, so the connections they dirtied render now
    (let [[conns _] (swap-vals! dirty (constantly #{}))]
      (render-all! sched (vec conns)))))

(defn- loop! [{:keys [wake tick-ms] :as sched}]
  (async/thread
    (loop [last-tick 0]
      (when (async/<!! wake)
        (let [wait (- (+ last-tick tick-ms) (now-ms))]
          ;; whatever else changes while we wait lands in this same tick
          (when (pos? wait) (Thread/sleep wait)))
        (let [start (now-ms)]
          (try (tick! sched)
               (catch Throwable t
                 (println "jolt.datastar: tick failed:" (ex-message t))))
          (recur start))))))

(defn scheduler
  "A render loop. Options:
    :tick-ms         minimum ms between ticks (default 15)
    :render-threads  workers a tick's renders are spread over (default 1:
                     render on the loop thread)
    :batch-fn        (fn [thunks]) running each tick's tx! thunks; default
                     runs them in order
    :cache           options for the component cache (see
                     jolt.datastar.cache/new-cache), or false for none
  The loop starts with the first stream or tx!."
  [{:keys [tick-ms render-threads batch-fn cache] :or {tick-ms 15 render-threads 1}}]
  {:tick-ms        tick-ms
   :render-threads render-threads
   :batch-fn       batch-fn
   :cache          (when-not (false? cache) (cache/new-cache (or cache {})))
   :wake           (async/chan (async/dropping-buffer 1))
   :dirty          (atom #{})
   :txs            (java.util.concurrent.LinkedBlockingQueue.)
   :started        (atom false)
   :work           (atom nil)})

(defn- ensure-started! [{:keys [started render-threads work] :as sched}]
  (when (compare-and-set! started false true)
    (when (> render-threads 1)
      (reset! work (start-workers! sched render-threads)))
    (loop! sched))
  sched)

(defn stop!
  "Stop the loop and its workers. Streams stop updating; tx! thunks queued
  after this never run."
  [{:keys [wake work render-threads]}]
  (async/close! wake)
  (when-let [q @work]
    (dotimes [_ render-threads] (.put ^java.util.concurrent.LinkedBlockingQueue q [nil nil])))
  nil)

(defn wake! [sched]
  (async/put! (:wake sched) true))

(defn mark-dirty! [sched conn]
  (swap! (:dirty sched) conj conn)
  (wake! sched))

(defn tx!
  "Queue thunk to run on the loop at the start of the next tick, before that
  tick renders. Returns a promise of its result (or the exception it threw)."
  [sched thunk]
  (ensure-started! sched)
  (let [p (promise)]
    (.put ^java.util.concurrent.LinkedBlockingQueue (:txs sched) [thunk p])
    (wake! sched)
    p))

(defn connect!
  "Open a stream: render (a no-argument fn returning the frame string, or nil
  to send nothing) runs on the loop whenever a ratom it read changes. Returns
  the channel the frames go out on; closing it (the client going away) ends
  the stream."
  [sched render]
  (ensure-started! sched)
  (let [ch      (async/chan (async/sliding-buffer 1))
        closed? (atom false)
        conn-p  (promise)
        watcher (fn watcher [cell]
                  (if @closed?
                    ;; the client is gone: let go of the cell that fired
                    (ratom/unwatch! cell watcher)
                    (mark-dirty! sched @conn-p)))
        conn    {:id        (gensym "conn")
                 :ch        ch
                 :render    render
                 :closed?   closed?
                 :prev-hash (atom nil)
                 :watcher   watcher}]
    (deliver conn-p conn)
    ;; the first frame goes out on the next tick
    (mark-dirty! sched conn)
    ch))

(defn stats
  "The component cache's counters, or nil without a cache."
  [{:keys [cache]}]
  (when cache (cache/stats cache)))
