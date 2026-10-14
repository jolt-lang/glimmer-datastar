;; Render-buffer strategies for jolt.datastar.html on jolt.
;;
;;   jolt -Sdeps '{:paths ["src" "bench"]}' bench/render.clj
;;
;; jolt's StringBuilder is a rope: append conses the piece, toString joins
;; them all in one copy, and a read by index (substring, charAt) first flushes
;; the pieces into a flat buffer. So the JVM habit of keeping one buffer per
;; render thread and cutting each render out of it may not pay here. Three
;; ways to get a page's HTML out of the same writer:
;;
;;   threadlocal  one builder per thread, reused; the render is cut out with
;;                substring and the builder truncated back
;;   fresh        a new builder per render, toString
;;   reset        one builder per thread, reused; truncated to zero and
;;                rendered from the start, then toString
(require '[jolt.datastar.html :as h])

(def write-node! @#'jolt.datastar.html/write-node!)

(def page
  [:div#board.grid
   [:h1 "Board"]
   [:table
    (for [i (range 300)]
      [:tr {:id (str "r" i) :class (when (even? i) "even") :data-on:click (str "@post('/row/" i "')")}
       [:td i] [:td (str "item " i " & more")] [:td {:style {:color "red"}} (* i i)]])]])

(def tl (ThreadLocal/withInitial (fn [] (StringBuilder. 16384))))

(defn r-threadlocal []
  (let [^StringBuilder sb (.get ^ThreadLocal tl)
        start (.length sb)]
    (write-node! sb page)
    (let [out (.substring sb start (.length sb))]
      (.setLength sb start)
      out)))

(defn r-fresh []
  (let [sb (StringBuilder.)]
    (write-node! sb page)
    (.toString sb)))

(defn r-reset []
  (let [^StringBuilder sb (.get ^ThreadLocal tl)]
    (.setLength sb 0)
    (write-node! sb page)
    (.toString sb)))

(defn now [] (System/nanoTime))

(defn time-per-render-us
  "Mean µs per render over n renders on each of `threads` threads at once."
  [f n threads]
  (dotimes [_ 200] (f))                       ; warm up
  (let [t0 (now)
        fs (doall (for [_ (range threads)] (future (dotimes [_ n] (f)))))]
    (run! deref fs)
    (/ (double (- (now) t0)) 1000.0 n threads)))

(let [size (count (r-fresh))]
  (assert (= (r-fresh) (r-threadlocal) (r-reset) (h/html page)))
  (println "page:" size "chars")
  (doseq [threads [1 4]]
    (doseq [[nm f] [["threadlocal" r-threadlocal] ["fresh" r-fresh] ["reset" r-reset]]]
      (let [runs (for [_ (range 3)] (time-per-render-us f 200 threads))]
        (println (format "%-12s threads=%d  best %8.1f µs/render  (wall %.1f µs per render per thread)"
                         nm threads (apply min runs) (* threads (apply min runs))))))))
(System/exit 0)
