(ns jolt.datastar.core
  "Datastar middleware for jolt: signals parsing, SSE live-render, and the
  page-init attributes. Speaks the datastar v1.0 wire protocol (JSON signals
  on actions, `datastar-request: true`, `datastar-patch-*` SSE events) and is
  self-contained — it parses query strings and bodies itself, so it needs no
  ring middleware stack.

  Server state lives in glimmer.ratom reactive cells (reagent semantics): each
  SSE stream renders the handler under glimmer.ratom/*current-watcher*, so
  every ratom dereffed during the render registers as a dependency, and any
  swap!/reset! on one marks the stream dirty. Dirty streams re-render together
  on the next tick (jolt.datastar.tick), once however many writes landed, and
  when the rendered HTML actually changed a `datastar-patch-elements` event is
  pushed — server-side reactivity in the reagent style. Components rendered
  with jolt.datastar.html are cached across streams.

        (def state (ratom/atom {:greetings []}))
        (def handler (datastar/wrap-datastar app-handler {:rate-limit-ms 15}))
        (adapter/run-server handler {:port 3000})
        ;; anywhere: (swap! state assoc :greetings ...) re-renders live pages

  The one host dependency is Chez Scheme itself: percent-decoding and hex
  formatting go through jolt.scheme bytevector/utf8 primitives."
  (:require
   [jolt.time] ; java.time shim — must load before clojure.data.json
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [jolt.datastar.html :as html]
   [jolt.datastar.tick :as tick]
   [jolt.scheme :as scheme]))

;; --- percent-decoding / hex (Chez bytevector + utf8 primitives) -------------

(defn- hex-value
  "Numeric value of one hex digit char."
  [c]
  (let [n (int c)]
    (cond
      (and (<= (int \0) n) (<= n (int \9))) (- n (int \0))
      (and (<= (int \a) n) (<= n (int \f))) (+ 10 (- n (int \a)))
      (and (<= (int \A) n) (<= n (int \F))) (+ 10 (- n (int \A)))
      :else (throw (ex-info (str "bad hex digit: " c) {:char c})))))

(defn- utf8-bytes
  "UTF-8 encoding of the single char c, as a vector of byte values."
  [c]
  (let [bv (scheme/call "string->utf8" (str c))
        n  (scheme/call "bytevector-length" bv)]
    (mapv (fn [i] (scheme/call "bytevector-u8-ref" bv i)) (range n))))

(defn- percent-decode
  "Decode percent-encoded UTF-8 string s. plus? additionally decodes + as space
  (form bodies); query strings keep + literal."
  [s plus?]
  (let [bytes (loop [i 0, acc []]
                (if (>= i (count s))
                  acc
                  (let [c (nth s i)]
                    (cond
                      (and (= c \+) plus?) (recur (inc i) (conj acc 32))
                      (and (= c \%) (<= (+ i 2) (dec (count s))))
                      (recur (+ i 3) (conj acc
                                          (+ (* 16 (hex-value (nth s (inc i))))
                                             (hex-value (nth s (+ i 2))))))
                      :else (recur (inc i) (into acc (utf8-bytes c)))))))]
    (if (empty? bytes)
      ""
      (let [bv (scheme/call "make-bytevector" (count bytes))]
        (doseq [[i b] (map-indexed vector bytes)]
          (scheme/call "bytevector-u8-set!" bv i b))
        (scheme/call "utf8->string" bv)))))

(defn- hex-str [n]
  (scheme/call "number->string" n 16))

;; --- signals ----------------------------------------------------------------

(defn- parse-signal-str
  "Signal name -> keyword. Dots and underscores both namespace:
  \"foo_bar\" -> :foo/bar, \"jolt.datastar.tab-id\" -> :jolt.datastar/tab-id,
  \"plain\" -> :plain. The serialize side (signal-name) inverts this exactly."
  [s]
  (let [segments (str/split s #"[._]")]
    (if (= (count segments) 1)
      (keyword s)
      (keyword (str/join "." (subvec segments 0 (dec (count segments))))
               (peek segments)))))

(defn- param-map
  "k=v pairs (& separated) into a map of decoded strings. plus? decodes + as
  space."
  [s plus?]
  (if (seq s)
    (reduce (fn [m pair]
              (if-let [i (str/index-of pair "=")]
                (assoc m (percent-decode (subs pair 0 i) plus?)
                         (percent-decode (subs pair (inc i)) plus?))
                m))
            {}
            (str/split s #"&"))
    {}))

(defn- query-params [request]
  (param-map (:query-string request) false))

(defn- body-text
  "The request body as a string (the adapter hands it over as a reader)."
  [request]
  (when-let [body (:body request)]
    (slurp body)))

(defn- signals-json-string
  "The datastar signals JSON from a request, or nil. GET carries it in the
  `datastar` query parameter; other methods as a JSON body, or in the
  `datastar` field of a form-encoded body."
  [{:keys [request-method headers] :as request}]
  (let [ct (get headers "content-type")]
    (cond
      (= :get request-method)
      (get (query-params request) "datastar")

      (and ct (str/includes? ct "application/json"))
      (body-text request)

      (and ct (str/includes? ct "application/x-www-form-urlencoded"))
      (get (param-map (body-text request) true) "datastar")

      :else nil)))

(defn- parse-signals
  "Signals map for a datastar request, nil when the request didn't come from
  datastar (no `datastar-request: true` header)."
  [{:keys [headers] :as request}]
  (when (= "true" (get headers "datastar-request"))
    (some-> (signals-json-string request)
            (json/read-str)
            (->> (walk/postwalk #(cond-> % (map? %)
                                   (update-keys (comp parse-signal-str name))))))))

(defn- jolt-signal
  "One of the signals init-opts declares. The client sends a dotted name back
  nested, {\"jolt\": {\"datastar\": {...}}}, which parses to nested maps;
  the flat :jolt.datastar/... key is still read for a client that sends it so."
  [signals k]
  (or (get signals (keyword "jolt.datastar" (name k)))
      (get-in signals [:jolt :datastar k])))

(defn- merge-signals
  "Parse datastar signals into the request and lift the tab id + CSRF token."
  [request]
  (let [signals    (parse-signals request)
        tab-id     (some-> (jolt-signal signals :tab-id) str parse-uuid)
        csrf-token (jolt-signal signals :anti-forgery-token)]
    (cond-> request
      signals (assoc :jolt.datastar/signals signals)
      tab-id  (assoc :jolt.datastar/tab-id tab-id)
      csrf-token (assoc-in [:headers "x-csrf-token"] csrf-token))))

(defn wrap-signals
  "Middleware: parse datastar signals into :jolt.datastar/signals on the
  request (plus :jolt.datastar/tab-id and the CSRF-token header bridge)."
  [handler]
  (fn [request]
    (handler (merge-signals request))))

(defn- signal-name-part
  "The wire name of one keyword (or value): `:foo/bar` -> \"foo_bar\",
  `:jolt.datastar/tab-id` -> \"jolt.datastar.tab-id\" (dotted namespaces keep
  dots, since dots mean nesting on the client: a path in an attribute name,
  nested objects in JSON, see signals-json)."
  [x]
  (if (keyword? x)
    (do
      (assert (not (str/includes? (name x) "_"))
              "Underscores are not allowed in signal keyword names.")
      (if-let [ns (namespace x)]
        (if (str/includes? ns ".")
          (str ns "." (name x))
          (str ns "_" (name x)))
        (name x)))
    (str x)))

(defn signal-name
  "The wire name of a signal keyword (or vector of keywords for a nested path)."
  [signal]
  (if (vector? signal)
    (str/join "." (mapv signal-name-part signal))
    (signal-name-part signal)))

(defn- key-json [k]
  (if (keyword? k)
    (signal-name-part k)
    (str k)))

(defn- merge-deep [a b]
  (if (and (map? a) (map? b)) (merge-with merge-deep a b) b))

(defn- wire-tree
  "Signals keyed by wire name, with a dotted name as nested maps: the client
  drops a JSON key that has a dot in it, leaving an empty object where the
  value should be."
  [signals]
  (reduce-kv (fn [m k v]
               (let [v (if (map? v) (wire-tree v) v)]
                 (update-in m (str/split (key-json k) #"\.") merge-deep v)))
             {}
             signals))

(defn signals-json
  "Signals map -> JSON string with wire signal names, a dotted name nested:
  {:jolt.datastar/tab-id x} is {\"jolt\": {\"datastar\": {\"tab-id\": x}}}.
  It comes back from the client nested the same way, so it parses to nested
  maps rather than to the dotted keyword."
  [signals]
  (json/write-str (wire-tree signals)))

(defn patch-signals
  "Ring response patching the client's signals. The datastar v1.0 client
  dispatches a datastar-patch-signals from a plain application/json body."
  [signals]
  {:status  200
   :headers {"Cache-Control" "no-store"
             "Content-Type"  "application/json"}
   :body    (signals-json signals)})

;; --- page init --------------------------------------------------------------

(def ^:private sse-param "datastar-sse")

(defn- sse-open-expr
  "The expression that opens the SSE stream for the current page. selector
  names the element the server patches (optional; default body).

  The @get waits for a microtask: datastar applies an element's attributes in
  the order they're written, and hiccup writes them sorted, data-init before
  data-signals, so a @get run at once would go out before the signals it
  should carry, the tab id among them, were set."
  [{:keys [selector]}]
  (str "queueMicrotask(() => @get("
       "location.pathname + "
       "(location.search + '&" sse-param "=true"
       (when selector
         (str "&datastar-selector=" (-> selector
                                        (str/replace "#" "%23")
                                        (str/replace " " "%20"))))
       "').replace(/^&/, '?'), "
       "{openWhenHidden: false, retryMaxCount: Infinity}))"))

(def ^:private tab-id-expr
  "JS for a fresh v4 UUID. crypto.randomUUID only exists in a secure context
  (https or localhost), and calling it over plain http to another host throws,
  which stops datastar from wiring up the rest of the page; getRandomValues
  exists everywhere."
  (str "(self.crypto.randomUUID ? self.crypto.randomUUID() : "
       "'10000000-1000-4000-8000-100000000000'.replace(/[018]/g, c => "
       "(c ^ self.crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)))"))

(defn init-opts
  "HTML attribute map for the page's datastar root element: data-signals seeds
  the per-tab id (plus any :signals you want initialized, and the CSRF token
  when given) and data-init opens the SSE stream. Options:
    :signals             map of signals to seed in data-signals
    :anti-forgery-token  CSRF token to round-trip through signals
    :selector            CSS selector of the element the SSE stream patches"
  ([]
   (init-opts {}))
  ([{:keys [signals anti-forgery-token selector]}]
   (merge {:data-signals
           (str "{"
                (str/join ","
                          (concat
                           (map (fn [[k v]]
                                  (str (json/write-str (name k)) ": " (json/write-str v)))
                                signals)
                           ;; nested, as the client drops a dotted key
                           [(str "'jolt': {'datastar': {'tab-id': " tab-id-expr
                                 (when anti-forgery-token
                                   (str ", 'anti-forgery-token': "
                                        (json/write-str anti-forgery-token)))
                                 "}}")]))
                "}")
           :data-init              (sse-open-expr {:selector selector})
           "data-on:online__window" (sse-open-expr {:selector selector})})))

;; --- SSE --------------------------------------------------------------------

(defn- elements-data-lines
  "data: lines for the `elements` argument. Multi-line HTML becomes repeated
  `data: elements <line>` lines, which the client re-joins with newlines."
  [html]
  (str "data: elements " (str/replace html "\n" "\ndata: elements ") "\n"))

(defn patch-elements-event
  "SSE datastar-patch-elements event: replace the target element's content
  (mode inner) with html, or the element itself (mode outer)."
  ([html selector]
   (patch-elements-event html selector "inner"))
  ([html selector mode]
   (str "event: datastar-patch-elements\n"
        "id: " (hex-str (hash html)) "\n"
        "data: selector " selector "\n"
        "data: mode " mode "\n"
        "data: namespace html\n"
        (elements-data-lines html)
        "data: useViewTransition false\n"
        "\n")))

(defn- sse-response
  "The ring response for an SSE request: a channel body the adapter streams,
  fed by the scheduler. Each frame renders the handler; a frame whose HTML is
  unchanged is not sent."
  [handler request sched]
  (let [q        (query-params request)
        selector (get q "datastar-selector" "body")
        mode     (get q "datastar-mode" "inner")
        request  (assoc request
                        :jolt.datastar/sse-request true
                        :jolt.datastar/selector selector
                        :jolt.datastar/mode mode)
        render   (fn []
                   (let [response (handler request)]
                     (when (= 200 (:status response))
                       (patch-elements-event (:body response) selector mode))))]
    {:status  200
     :headers {"Content-Type"  "text/event-stream; charset=utf-8"
               "Cache-Control" "no-store"}
     :body    (tick/connect! sched render)}))

;; --- the entry point --------------------------------------------------------

(def default-options
  {:tick-ms        15
   :render-threads 1
   :sse-param      sse-param})

(defn scheduler
  "A render loop to share between wrap-datastar and code outside a request:
  pass it as :scheduler and call (tx! sched thunk) from anywhere. Takes the
  same :tick-ms, :render-threads, :batch-fn and :cache options as
  wrap-datastar."
  ([] (scheduler {}))
  ([opts] (tick/scheduler (merge default-options opts))))

(defn stop!
  "Stop a scheduler's render loop and workers."
  [sched]
  (tick/stop! sched))

(defn tx!
  "Queue thunk to run at the start of the next render tick, before anything
  renders, and return a promise of its result (or of the exception it threw).
  The first argument is the request (wrap-datastar puts its scheduler on it)
  or a scheduler.

  This is opt-in. A plain swap! still works and still re-renders on the next
  tick; what tx! adds is that writes and renders take turns, so a render never
  sees half of an update made of several swaps, and a :batch-fn can apply a
  tick's writes together."
  [request-or-sched thunk]
  (tick/tx! (or (:jolt.datastar/scheduler request-or-sched) request-or-sched) thunk))

(defn cache-stats
  "The component cache's counters (hits, misses, stale, admitted, entries) for
  the request's (or a) scheduler, or nil without a cache."
  [request-or-sched]
  (tick/stats (or (:jolt.datastar/scheduler request-or-sched) request-or-sched)))

(defn wrap-datastar
  "The datastar middleware entry point. Wrap the app handler with it, passing
  a params map of options:
    :tick-ms         minimum ms between renders of the open streams (default
                     15; :rate-limit-ms is read as an older name for it)
    :render-threads  threads a tick's renders are spread over (default 1)
    :batch-fn        (fn [thunks]) applying each tick's tx! thunks (default:
                     each in order)
    :cache           component cache options, or false to render components
                     uncached (see jolt.datastar.cache/new-cache)
    :scheduler       a scheduler made with `scheduler`, instead of a new one
                     built from the options above
    :sse-param       query parameter that marks an SSE request (default
                     \"datastar-sse\")

  Signals are parsed into :jolt.datastar/signals; SSE requests (which the page
  opens via init-opts' data-init) get a streaming response that re-renders the
  handler on the next tick after a glimmer ratom it derefs changes. Every
  request carries the scheduler as :jolt.datastar/scheduler (see tx!), and
  renders with the component cache bound (see jolt.datastar.html)."
  [handler opts]
  (let [opts  (cond-> (merge default-options opts)
                (and (:rate-limit-ms opts) (not (:tick-ms opts)))
                (assoc :tick-ms (:rate-limit-ms opts)))
        sched (or (:scheduler opts) (tick/scheduler opts))
        cache (:cache sched)]
    (fn [request]
      (let [request (-> (merge-signals request)
                        (assoc :jolt.datastar/scheduler sched))]
        (if (= "true" (get (query-params request) (:sse-param opts)))
          (sse-response handler request sched)
          (binding [html/*cache* cache]
            (handler request)))))))
