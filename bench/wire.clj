;; Octets on the wire per frame, with the stream compression in
;; ring-chez-adapter's wrap-gzip (v0.7.14+), which this library does not
;; depend on, so put the adapter on the path:
;;
;;   jolt -Sdeps '{:paths ["src" "bench" "../ring-chez-adapter/src"]}' bench/wire.clj
;;
;; Frames are the board page from bench/ticks.clj, one row changing per frame.
(require '[jolt.datastar.core :as ds]
         '[jolt.datastar.html :as h]
         '[ring-chez.zlib :as zlib])

(defn row [i v]
  [:tr {:id (str "r" i) :data-on:click (str "@post('/row/" i "')")}
   [:td i] [:td (str "item " v " & more")] [:td {:style {:color "red"}} (* v v)]])

(defn frame [board]
  (ds/patch-elements-event
   (h/html [:div#board [:table (map-indexed row board)]]) "#board" "inner"))

(defn bytes-of ^bytes [^String s] (.getBytes s "UTF-8"))

(defn measure [rows]
  (let [boards  (reductions (fn [b i] (update b (mod (* 7 i) rows) inc)) (vec (range rows)) (range 20))
        frames  (mapv frame boards)
        raw     (mapv #(alength (bytes-of %)) frames)
        oneshot (mapv #(alength ^bytes (zlib/gzip (bytes-of %))) frames)
        d       (zlib/deflater)
        stream  (mapv #(alength (zlib/deflate-chunk! d (bytes-of %))) frames)
        avg     (fn [xs] (quot (reduce + (rest xs)) (dec (count xs))))]
    (zlib/deflate-close! d)
    (println (format "%3d rows: frame %6d octets | gzip per frame %5d | gzip stream %5d first, %5d per changed frame"
                     rows (first raw) (avg oneshot) (first stream) (avg stream)))))

;; deflate's window is 32 KB: a frame that fits is matched against the whole
;; previous frame, one that does not only partly
(run! measure [50 150 220 300])
(System/exit 0)
