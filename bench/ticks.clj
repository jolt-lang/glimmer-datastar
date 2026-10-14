;; How long one tick takes to re-render every open stream, by render-threads
;; and with the component cache on and off.
;;
;;   jolt -Sdeps '{:paths ["src" "bench"]}' bench/ticks.clj
;;
;; Each stream shows the same board (300 rows as components) plus a line of its
;; own, the shape of a multiplayer page: a lot of shared state, a little per
;; viewer. One row changes per tick.
(require '[clojure.core.async :as async]
         '[glimmer.ratom :as ratom]
         '[jolt.datastar.core :as ds]
         '[jolt.datastar.html :as h])

(def streams (or (some-> (System/getenv "STREAMS") parse-long) 100))
(def ticks 10)

(def board (ratom/atom (vec (range 300))))

(defn row [i v]
  [:tr {:id (str "r" i) :data-on:click (str "@post('/row/" i "')")}
   [:td i] [:td (str "item " v " & more")] [:td {:style {:color "red"}} (* v v)]])

(defn rows []
  [:table (map-indexed (fn [i v] [row i v]) @board)])

(defn page [viewer]
  (h/html [:div#board [:p "viewer " viewer] [rows]]))

(defn now [] (System/nanoTime))

(defn run [render-threads cache?]
  (let [sched   (ds/scheduler {:tick-ms 1 :render-threads render-threads
                               :cache (if cache? {} false)})
        handler (ds/wrap-datastar (fn [req] {:status 200 :body (page (:viewer req))})
                                  {:scheduler sched})
        chs     (vec (for [v (range streams)]
                       (:body (handler {:request-method :get :query-string "datastar-sse=true"
                                        :headers {} :uri "/" :viewer v}))))
        drain!  (fn [] (run! #(async/<!! %) chs))]
    (drain!)                                   ; first frames
    (dotimes [_ 2] (swap! board update 0 inc) (drain!)) ; warm the cache
    (let [t0 (now)]
      (dotimes [i ticks]
        (swap! board update (mod (* 7 i) 300) inc)
        (drain!))
      (let [ms (/ (double (- (now) t0)) 1e6 ticks)]
        (run! async/close! chs)
        (ds/stop! sched)
        ms))))

(println "streams:" streams)
(doseq [cache? [false true]
        threads [1 2 4 8]]
  (println (format "cache %-5s render-threads %d  %8.1f ms/tick"
                   cache? threads (run threads cache?))))
(System/exit 0)
