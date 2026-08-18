(ns jolt.datastar.core
  "Datastar middleware for jolt: signals parsing, SSE live-render, and the
  page-init attributes. Speaks the datastar v1.0 wire protocol (JSON signals
  on actions, `datastar-request: true`, `datastar-patch-*` SSE events) and is
  self-contained — it parses query strings and bodies itself, so it needs no
  ring middleware stack.

  Server state lives in glimmer.ratom reactive cells (reagent semantics): the
  SSE loop renders the handler under glimmer.ratom/*current-watcher*, so every
  ratom dereffed during the render registers as a dependency, and any
  swap!/reset! on one re-renders the stream. When the rendered HTML actually
  changed, a `datastar-patch-elements` event is pushed to every open stream —
  server-side reactivity in the reagent style.

        (def state (ratom/atom {:greetings []}))
        (def handler (datastar/wrap-datastar app-handler {:rate-limit-ms 15}))
        (adapter/run-server handler {:port 3000})
        ;; anywhere: (swap! state assoc :greetings ...) re-renders live pages

  The one host dependency is Chez Scheme itself: percent-decoding and hex
  formatting go through jolt.scheme bytevector/utf8 primitives."
  (:require
   [jolt.time] ; java.time shim — must load before clojure.data.json
   [glimmer.ratom :as ratom]
   [clojure.core.async :as async]
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.walk :as walk]
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

(defn- merge-signals
  "Parse datastar signals into the request and lift the tab id + CSRF token."
  [request]
  (let [signals    (parse-signals request)
        tab-id     (some-> (:jolt.datastar/tab-id signals) parse-uuid)
        csrf-token (:jolt.datastar/anti-forgery-token signals)]
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
  dots, since dots mean nesting on the client and the parse side is symmetric)."
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

(defn signals-json
  "Signals map -> JSON string with wire signal names."
  [signals]
  (json/write-str signals :key-fn key-json))

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
  "The @get expression that opens the SSE stream for the current page. selector
  names the element the server patches (optional; default body)."
  [{:keys [selector]}]
  (str "@get("
       "location.pathname + "
       "(location.search + '&" sse-param "=true"
       (when selector
         (str "&datastar-selector=" (-> selector
                                        (str/replace "#" "%23")
                                        (str/replace " " "%20"))))
       "').replace(/^&/, '?'), "
       "{openWhenHidden: false, retryMaxCount: Infinity})"))

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
                           [(str "'jolt.datastar.tab-id': self.crypto.randomUUID()"
                                 (when anti-forgery-token
                                   (str ", 'jolt.datastar.anti-forgery-token': "
                                        (json/write-str anti-forgery-token))))]))
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

(defn- sse-body
  "A channel streaming SSE patch events for one connection. Each pass renders
  the handler under glimmer's *current-watcher*, so ratoms dereffed during the
  render subscribe this connection; the loop parks until one of them changes,
  re-renders, and pushes an event when the HTML changed. Closing the client
  connection ends the loop (and, on the next state change, unregisters its
  stale watcher)."
  [handler request {:keys [rate-limit-ms selector mode]}]
  (let [ch    (async/chan)
        dirty (async/chan 1)]
    (letfn [(tick [cell]
              ;; put! returns false once dirty is closed (client gone);
              ;; unregister from the cell that fired us
              (when-not (async/put! dirty true)
                (ratom/unwatch! cell tick)))]
      (async/go
        (try
          (loop [prev-hash nil]
            (let [response (binding [ratom/*current-watcher* tick]
                             (handler request))
                  body     (when (= 200 (:status response)) (:body response))
                  h        (if body (hash body) prev-hash)]
              (cond
                (and (not= h prev-hash)
                     (not (async/>! ch (patch-elements-event body selector mode))))
                nil ; client went away

                (not= h prev-hash)
                (do (async/<! (async/timeout rate-limit-ms))
                    (when (async/<! dirty) (recur h)))

                :else
                (when (async/<! dirty) (recur h)))))
          (finally (async/close! dirty)))))
    ch))

(defn- sse-response
  "The ring response for an SSE request: a channel body the adapter streams."
  [handler request {:keys [rate-limit-ms]}]
  (let [request (assoc request
                       :jolt.datastar/sse-request true
                       :jolt.datastar/selector (get (query-params request)
                                                    "datastar-selector" "body")
                       :jolt.datastar/mode (get (query-params request)
                                                "datastar-mode" "inner"))]
    {:status  200
     :headers {"Content-Type"  "text/event-stream; charset=utf-8"
               "Cache-Control" "no-store"}
     :body    (sse-body handler request {:rate-limit-ms rate-limit-ms
                                         :selector (:jolt.datastar/selector request)
                                         :mode     (:jolt.datastar/mode request)})}))

;; --- the entry point --------------------------------------------------------

(def default-options
  {:rate-limit-ms 15
   :sse-param     sse-param})

(defn wrap-datastar
  "The datastar middleware entry point. Wrap the app handler with it, passing
  a params map of options:
    :rate-limit-ms  minimum ms between SSE re-renders (default 15)
    :sse-param      query parameter that marks an SSE request (default
                    \"datastar-sse\")

  Signals are parsed into :jolt.datastar/signals; SSE requests (which the page
  opens via init-opts' data-init) get a streaming response that re-renders the
  handler whenever a glimmer ratom it derefs changes."
  [handler opts]
  (let [opts (merge default-options opts)]
    (fn [request]
      (let [request (merge-signals request)]
        (if (= "true" (get (query-params request) (:sse-param opts)))
          (sse-response handler request opts)
          (handler request))))))
