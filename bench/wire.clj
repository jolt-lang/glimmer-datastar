;; Octets on the wire per frame, with the stream compression in
;; ring-chez-adapter's wrap-gzip (v0.7.14+), which this library does not
;; depend on, so put the adapter on the path:
;;
;;   jolt -Sdeps '{:paths ["src" "bench" "../ring-chez-adapter/src"]}' bench/wire.clj
;;
;; Frames are the board page from bench/ticks.clj, one row changing per frame:
;; whole pages, and then what a stream sends when the rows are components,
;; which is the changed row alone.
(require '[clojure.core.async :as async]
         '[glimmer.ratom :as ratom]
         '[jolt.datastar.core :as ds]
         '[jolt.datastar.html :as h]
         '[ring-chez.zlib :as zlib])

(defn row [i v]
  [:tr {:id (str "r" i) :data-on:click (str "@post('/row/" i "')")}
   [:td i] [:td (str "item " v " & more")] [:td {:style {:color "red"}} (* v v)]])

(defn frame [board]
  (ds/patch-elements-event
   (h/html [:div#board [:table (map-indexed row board)]]) "#board" "inner"))

(defn bytes-of ^bytes [^String s] (.getBytes s "UTF-8"))

(defn patch-frames
  "The frames a stream sends while the board goes through boards."
  [boards]
  (let [board   (ratom/atom (first boards))
        sched   (ds/scheduler {:tick-ms 1})
        handler (ds/wrap-datastar
                 (fn [_] {:status 200
                          :body (h/html [:div#board [:table (map-indexed (fn [i v] [row i v]) @board)]])})
                 {:scheduler sched})
        ch      (:body (handler {:request-method :get :query-string "datastar-sse=true"
                                 :headers {} :uri "/"}))
        frames  (into [(async/<!! ch)]
                      (map (fn [b] (reset! board b) (async/<!! ch)))
                      (rest boards))]
    (async/close! ch)
    (ds/stop! sched)
    frames))

(defn measure [rows]
  (let [boards  (reductions (fn [b i] (update b (mod (* 7 i) rows) inc)) (vec (range rows)) (range 20))
        frames  (mapv frame boards)
        raw     (mapv #(alength (bytes-of %)) frames)
        oneshot (mapv #(alength ^bytes (zlib/gzip (bytes-of %))) frames)
        d       (zlib/deflater)
        stream  (mapv #(alength (zlib/deflate-chunk! d (bytes-of %))) frames)
        patches (patch-frames boards)
        pd      (zlib/deflater)
        pstream (mapv #(alength (zlib/deflate-chunk! pd (bytes-of %))) patches)
        avg     (fn [xs] (quot (reduce + (rest xs)) (dec (count xs))))]
    (zlib/deflate-close! d)
    (zlib/deflate-close! pd)
    (println (format "%3d rows: frame %6d octets | gzip per frame %5d | gzip stream %5d first, %5d per changed frame | patch %4d raw, %4d gzip stream"
                     rows (first raw) (avg oneshot) (first stream) (avg stream)
                     (avg (mapv #(alength (bytes-of %)) patches)) (avg pstream)))))

;; deflate's window is 32 KB: a frame that fits is matched against the whole
;; previous frame, one that does not only partly
(run! measure [50 150 220 300])
(System/exit 0)
