(ns jolt.datastar.tick-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [glimmer.ratom :as ratom]
            [jolt.datastar.core :as ds]
            [jolt.datastar.html :as h]))

(defn- sse-request []
  {:request-method :get :query-string "datastar-sse=true" :headers {} :uri "/"})

(defn- take-frame
  "The next frame on ch, or :timeout."
  [ch]
  (let [[v _] (async/alts!! [ch (async/timeout 2000)])]
    (if (nil? v) :timeout v)))

(defn- quiet?
  "No frame arrives on ch within ms."
  [ch ms]
  (let [[v port] (async/alts!! [ch (async/timeout ms)])]
    (not (and (= port ch) v))))

(deftest a-burst-of-writes-renders-once
  ;; every swap! used to queue its own wakeup, so W writes meant W renders per
  ;; stream; now they land in one tick
  (let [state   (ratom/atom 0)
        renders (atom 0)
        sched   (ds/scheduler {:tick-ms 50})
        handler (ds/wrap-datastar (fn [_]
                                    (swap! renders inc)
                                    {:status 200 :body (str "<p>" @state "</p>")})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (is (str/includes? (take-frame ch) "<p>0</p>"))
    (is (= 1 @renders))
    (dotimes [i 100] (swap! state inc))
    (is (str/includes? (take-frame ch) "<p>100</p>"))
    (is (quiet? ch 200))
    (is (= 2 @renders) "one render for the whole burst")
    (async/close! ch)
    (ds/stop! sched)))

(deftest tx-runs-before-the-tick-renders
  (let [a (ratom/atom 0)
        b (ratom/atom 0)
        sched   (ds/scheduler {:tick-ms 20})
        seen    (atom [])
        handler (ds/wrap-datastar (fn [_]
                                    (swap! seen conj [@a @b])
                                    {:status 200 :body (str @a "/" @b)})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (let [p (ds/tx! sched (fn [] (swap! a inc) (swap! b inc) :done))]
      (is (= :done (deref p 2000 :timeout)))
      (is (str/includes? (take-frame ch) "1/1"))
      ;; writes and renders take turns: no render saw a without b
      (is (every? (fn [[x y]] (= x y)) @seen)))
    (testing "a thunk that throws delivers the exception"
      (is (instance? Throwable (deref (ds/tx! sched #(throw (ex-info "no" {}))) 2000 :timeout))))
    (testing "tx! takes the request too"
      (let [req-sched (atom nil)
            h (ds/wrap-datastar (fn [req] (reset! req-sched req) {:status 200 :body ""})
                                {:scheduler sched})]
        (h {:request-method :post :headers {} :uri "/"})
        (is (= :ok (deref (ds/tx! @req-sched (constantly :ok)) 2000 :timeout)))))
    (async/close! ch)
    (ds/stop! sched)))

(deftest batch-fn-sees-a-tick-of-thunks-together
  (let [batches (atom [])
        sched   (ds/scheduler {:tick-ms 100
                               :batch-fn (fn [thunks]
                                           (swap! batches conj (count thunks))
                                           (run! #(%) thunks))})
        hits    (atom 0)
        ps      (doall (for [_ (range 5)] (ds/tx! sched #(swap! hits inc))))]
    (run! #(deref % 2000 :timeout) ps)
    (is (= 5 @hits))
    (is (= 5 (reduce + @batches)))
    (is (< (count @batches) 5) "thunks queued within a tick run as one batch")
    (ds/stop! sched)))

(deftest render-threads-render-every-stream
  (let [state   (ratom/atom 0)
        sched   (ds/scheduler {:tick-ms 10 :render-threads 4})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str "<p>" @state "</p>")})
                                  {:scheduler sched})
        chs     (vec (repeatedly 20 #(:body (handler (sse-request)))))]
    (is (every? #(str/includes? (take-frame %) "<p>0</p>") chs))
    (reset! state 7)
    (is (every? #(str/includes? (take-frame %) "<p>7</p>") chs))
    (run! async/close! chs)
    (ds/stop! sched)))

(deftest a-slow-client-gets-the-newest-frame
  (let [state   (ratom/atom 0)
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str "<p>" @state "</p>")})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    ;; nobody reads while three ticks' worth of changes go by
    (dotimes [i 3] (reset! state (inc i)) (Thread/sleep 30))
    (is (str/includes? (take-frame ch) "<p>3</p>") "stale frames were dropped")
    (async/close! ch)
    (ds/stop! sched)))

(deftest a-closed-stream-stops-rendering
  (let [state   (ratom/atom 0)
        renders (atom 0)
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] (swap! renders inc)
                                    {:status 200 :body (str @state)})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (async/close! ch)
    (swap! state inc)      ; renders once more, finds the channel closed
    (Thread/sleep 50)
    (let [n @renders]
      (swap! state inc)    ; the watcher lets go instead of re-rendering
      (swap! state inc)
      (Thread/sleep 50)
      (is (= n @renders)))
    (ds/stop! sched)))

(deftest components-are-shared-between-streams
  (let [state   (ratom/atom "x")
        calls   (atom 0)
        board   (fn [] (swap! calls inc) [:div @state])
        ;; one thread, for an exact count: two render threads can both miss
        ;; the same component before either has stored it
        sched   (ds/scheduler {:tick-ms 10 :render-threads 1})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (jolt.datastar.html/html [board])})
                                  {:scheduler sched})
        chs     (vec (repeatedly 10 #(:body (handler (sse-request)))))]
    (is (every? #(str/includes? (take-frame %) "<div>x</div>") chs))
    (is (<= @calls 2) "rendered on first sight and on admission, then shared")
    (reset! calls 0)
    (reset! state "y")
    (is (every? #(str/includes? (take-frame %) "<div>y</div>") chs)
        "pages served from the cache still update")
    (is (<= @calls 1))
    (run! async/close! chs)
    (ds/stop! sched)))

(deftest cached-components-stay-correct-across-render-threads
  (let [state   (ratom/atom 0)
        cell    (fn [i] [:td i "/" @state])
        sched   (ds/scheduler {:tick-ms 5 :render-threads 4})
        handler (ds/wrap-datastar (fn [req]
                                    {:status 200
                                     :body (jolt.datastar.html/html
                                            [:tr [:th (:viewer req)] (for [i (range 5)] [cell i])])})
                                  {:scheduler sched})
        chs     (vec (for [v (range 12)]
                       (:body (handler (assoc (sse-request) :viewer v)))))]
    (is (every? #(str/includes? (take-frame %) "<td>4/0</td>") chs))
    (doseq [n (range 1 6)]
      (reset! state n)
      (is (every? (fn [[v ch]]
                    (let [f (take-frame ch)
                          ok (and (str/includes? f (str "<th>" v "</th>"))
                                  (str/includes? f (str "<td>4/" n "</td>")))]
                      (when-not ok (println "stream" v "state" n "got" (pr-str f)))
                      ok))
                  (map-indexed vector chs))
          (str "every stream sees its own header and state " n)))
    (run! async/close! chs)
    (ds/stop! sched)))

(defn- watchers [cell] (count @(:watches cell)))

(deftest a-closed-stream-lets-go-of-its-cells
  ;; the stream used to hear of the close only from a put! that failed, which
  ;; never came when no cell it read changed again or its HTML stayed the
  ;; same, so a closed page stayed on its cells for as long as they lived
  (let [state   (ratom/atom 0)
        other   (ratom/atom :x)
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str @state @other)})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (is (= 1 (watchers state)))
    (is (= 1 (watchers other)))
    (async/close! ch)
    (is (= 0 (watchers state)) "released on close, with nothing changed")
    (is (= 0 (watchers other)))
    (ds/stop! sched)))

(deftest a-stream-watches-only-what-it-last-read
  (let [which   (ratom/atom :a)
        a       (ratom/atom "a")
        b       (ratom/atom "b")
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str (if (= :a @which) @a @b))})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (is (= [1 0] [(watchers a) (watchers b)]))
    (reset! which :b)
    (is (str/includes? (take-frame ch) "b"))
    (is (= [0 1] [(watchers a) (watchers b)]) "a no longer shapes the page")
    (async/close! ch)
    (ds/stop! sched)))

(deftest a-refused-render-ends-the-stream
  ;; a 401 once the session the stream came from has ended, say
  (let [signed-in (ratom/atom true)
        sched     (ds/scheduler {:tick-ms 5})
        handler   (ds/wrap-datastar (fn [_] (if @signed-in
                                              {:status 200 :body "<p>mine</p>"}
                                              {:status 401 :body "sign in"}))
                                    {:scheduler sched})
        ch        (:body (handler (sse-request)))]
    (is (str/includes? (take-frame ch) "mine"))
    (reset! signed-in false)
    (is (= [nil ch] (async/alts!! [ch (async/timeout 2000)])) "the channel closes")
    (is (= 0 (watchers signed-in)))
    (ds/stop! sched)))

(deftest a-stream-closed-from-a-fiber-lets-go-too
  ;; the fiber server closes the channel from a fiber, holding the channel's
  ;; lock, where blocking is refused: the release must not block
  (let [state   (ratom/atom 0)
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str @state)})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (is (= 1 (watchers state)))
    (async/<!! (async/io-thread (async/close! ch)))
    (is (= 0 (watchers state)))
    (ds/stop! sched)))

(defn- board-row [i v] [:tr {:id (str "r" i)} [:td i] [:td v]])

(defn- board-handler [board]
  (fn [_] {:status 200
           :body (h/html [:table#board (map-indexed (fn [i v] [board-row i v]) @board)])}))

(deftest a-changed-component-is-patched-alone
  (let [board   (ratom/atom (vec (range 50)))
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (board-handler board) {:scheduler sched})
        ch      (:body (handler (sse-request)))
        first-f (take-frame ch)]
    (is (str/includes? first-f "data: selector body") "the first frame is the whole page")
    (swap! board assoc 7 :seven)
    (let [f (take-frame ch)]
      (is (str/includes? f "<tr id=\"r7\"><td>7</td><td>seven</td></tr>"))
      (is (not (str/includes? f "r8")) "only the changed row")
      (is (not (str/includes? f "data: selector")) "patched by id")
      (is (str/includes? f "data: mode outer")))
    (testing "two rows in a tick go out in one patch"
      (swap! board assoc 1 :one 2 :two)
      (let [f (take-frame ch)]
        (is (str/includes? f "<td>one</td>"))
        (is (str/includes? f "<td>two</td>"))
        (is (not (str/includes? f "data: selector")))))
    (testing "a row added changes the page around the rows: the whole page"
      (swap! board conj :new)
      (let [f (take-frame ch)]
        (is (str/includes? f "data: selector body"))
        (is (str/includes? f "<tr id=\"r50\">"))))
    (async/close! ch)
    (ds/stop! sched)))

(deftest a-dropped-patch-is-made-up-with-the-whole-page
  ;; frames slide: a patch the client never took is gone, so the next frame
  ;; can't be a patch against a page the client hasn't got
  (let [board   (ratom/atom (vec (range 10)))
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (board-handler board) {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (swap! board assoc 1 :one)
    (Thread/sleep 50)                   ; rendered, not taken
    (swap! board assoc 2 :two)
    (Thread/sleep 50)
    (let [f (take-frame ch)]
      (is (str/includes? f "data: selector body"))
      (is (str/includes? f "<td>one</td>"))
      (is (str/includes? f "<td>two</td>")))
    (async/close! ch)
    (ds/stop! sched)))

(deftest duplicate-ids-get-the-whole-page
  (let [state   (ratom/atom 0)
        twin    (fn [] [:p#same @state])
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (h/html [:div [twin] [twin]])})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (reset! state 1)
    (is (str/includes? (take-frame ch) "data: selector body"))
    (async/close! ch)
    (ds/stop! sched)))

(deftest component-patches-can-be-turned-off
  (let [board   (ratom/atom (vec (range 5)))
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (board-handler board)
                                  {:scheduler sched :component-patches false})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (swap! board assoc 3 :three)
    (is (str/includes? (take-frame ch) "data: selector body"))
    (async/close! ch)
    (ds/stop! sched)))

(deftest a-page-not-from-html-is-sent-whole
  (let [state   (ratom/atom 0)
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str "<p>" @state "</p>")})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (reset! state 1)
    (is (str/includes? (take-frame ch) "data: selector body"))
    (async/close! ch)
    (ds/stop! sched)))

(deftest a-patch-beside-an-unchanged-group
  (let [title   (ratom/atom "a")
        note    (ratom/atom "x")
        board   (ratom/atom (vec (range 5)))
        header  (fn [] [:h1#title @title])
        rows    (fn [] [:table (map-indexed (fn [i v] [board-row i v]) @board)])
        sched   (ds/scheduler {:tick-ms 5})
        handler (ds/wrap-datastar (fn [_] {:status 200
                                           :body (h/html [:div [header] [:p @note] [rows]])})
                                  {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (dotimes [_ 3] (swap! board identity) (reset! title (str @title "!")) (take-frame ch))
    (reset! title "b")
    (let [f (take-frame ch)]
      (is (str/includes? f "<h1 id=\"title\">b</h1>"))
      (is (not (str/includes? f "<tr")) "the rows, served whole from the cache, are left alone")
      (is (not (str/includes? f "data: selector"))))
    (testing "text outside any part changed: the whole page"
      (reset! note "y")
      (let [f (take-frame ch)]
        (is (str/includes? f "data: selector body"))
        (is (str/includes? f "<p>y</p>"))))
    (async/close! ch)
    (ds/stop! sched)))

(deftest patches-without-the-cache
  (let [board   (ratom/atom (vec (range 5)))
        sched   (ds/scheduler {:tick-ms 5 :cache false})
        handler (ds/wrap-datastar (board-handler board) {:scheduler sched})
        ch      (:body (handler (sse-request)))]
    (take-frame ch)
    (swap! board assoc 3 :three)
    (let [f (take-frame ch)]
      (is (str/includes? f "<td>three</td>"))
      (is (not (str/includes? f "r2")))
      (is (not (str/includes? f "data: selector"))))
    (async/close! ch)
    (ds/stop! sched)))
