(ns jolt.datastar.cache
  "The rendered-component cache: a bounded map from a component vector
  ([f & args]) to the HTML it rendered, shared by every connection, so a
  component that a thousand open pages show is rendered once per change rather
  than once per page.

  Entries are admitted on their second request. A component rendered once and
  never again (a row keyed by a timestamp, say) would otherwise evict ones that
  are asked for every frame; its first render goes through uncached and only
  leaves a note that it was seen. The note is a hash in a set that is dropped
  whole when it fills, which is the doorkeeper of TinyLFU without the counting
  sketch.

  Eviction is by generation: hits are served from the young map, or from the
  old one and promoted. When the young map's HTML passes half the budget it
  becomes the old map and the previous old map is dropped, so what goes is
  whatever was not asked for during a whole generation — LRU, approximately,
  without per-entry bookkeeping on the hit path.

  An entry also records the reactive cells the component read and the values
  it read from them (glimmer.ratom/capture). It is only good while those cells
  still hold those values, and a hit re-reads them, which subscribes the
  connection being rendered as if the component had run.")

(defn new-cache
  "An empty cache. Options:
    :max-chars    HTML held across both generations, in characters
                  (default 16M)
    :doorkeeper   keys remembered as seen once before the note is dropped
                  (default 100000)"
  ([] (new-cache {}))
  ([{:keys [max-chars doorkeeper] :or {max-chars (* 16 1024 1024) doorkeeper 100000}}]
   {:max-chars  max-chars
    :doorkeeper doorkeeper
    :state      (atom {:young {} :young-chars 0 :old {} :seen #{}})
    :stats      (atom {:hits 0 :misses 0 :stale 0 :admitted 0})}))

(defn lookup
  "The entry for k, or nil. An entry found in the old generation is promoted."
  [{:keys [state]} k]
  (let [{:keys [young old]} @state]
    (or (get young k)
        (when-let [e (get old k)]
          (swap! state (fn [s]
                         (if (contains? (:young s) k)
                           s
                           (-> s
                               (update :young assoc k e)
                               (update :young-chars + (count (:html e)))))))
          e))))

(defn- rotate [{:keys [young young-chars] :as s} max-chars]
  (if (> young-chars (quot max-chars 2))
    (assoc s :old young :young {} :young-chars 0)
    s))

(defn admit!
  "Offer entry e for k. It is stored when k has been offered before (or is
  already cached, i.e. being refreshed); otherwise k is only noted as seen.
  Returns true when stored."
  [{:keys [state stats max-chars doorkeeper]} k e]
  (let [h (hash k)
        stored? (volatile! false)]
    (swap! state
           (fn [{:keys [young old seen] :as s}]
             (vreset! stored? false)
             (cond
               (or (contains? young k) (contains? old k) (contains? seen h))
               (do (vreset! stored? true)
                   (-> s
                       (update :young assoc k e)
                       ;; a refresh replaces the young copy, if any
                       (update :young-chars + (- (count (:html e))
                                                 (count (:html (get young k) ""))))
                       (update :seen disj h)
                       (rotate max-chars)))

               (>= (count seen) doorkeeper)
               (assoc s :seen #{h})

               :else
               (update s :seen conj h))))
    (when @stored? (swap! stats update :admitted inc))
    @stored?))

(defn record!
  "Count a hit, a miss or a stale entry (:hits / :misses / :stale)."
  [{:keys [stats]} what]
  (swap! stats update what inc))

(defn stats
  "Counters and sizes, for tuning."
  [{:keys [state stats]}]
  (let [{:keys [young old young-chars seen]} @state]
    (assoc @stats
           :entries (+ (count young) (count old))
           :young-chars young-chars
           :seen (count seen))))

(defn clear!
  "Drop every entry."
  [{:keys [state]}]
  (reset! state {:young {} :young-chars 0 :old {} :seen #{}}))
