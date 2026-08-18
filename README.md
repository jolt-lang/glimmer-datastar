# glimmer-datastar

Datastar middleware for [jolt](https://github.com/jolt-lang/jolt). It implements
the server side of the Datastar v1.0 wire protocol: JSON signals on actions, the
`datastar-request: true` header, and `datastar-patch-*` SSE events. It parses
query strings and request bodies itself, so it needs no ring middleware stack.

## How it works

Server state lives in `glimmer.ratom` reactive cells (reagent-style atoms). Each
SSE connection renders the handler under glimmer's `*current-watcher*`, so every
ratom dereffed during the render subscribes that connection. When a subscribed
ratom changes, the loop re-renders the handler and, if the HTML actually
changed, pushes a `datastar-patch-elements` event to every open stream. Swap a
ratom anywhere in the app and all live pages update, reagent style.

The only host dependency is Chez Scheme itself, used for percent-decoding and
hex formatting through jolt.scheme bytevector and UTF-8 primitives.

## Usage

Wrap the app handler with `wrap-datastar`, keep page state in a ratom, and run
the server:

```clojure
(require '[jolt.datastar.core :as datastar]
         '[glimmer.ratom :as ratom]
         '[ring-chez.adapter :as adapter])

(def state (ratom/atom {:greetings []}))

(def handler
  (-> (fn [_request]
        {:status 200
         :body (str "<ul>"
                    (apply str (map #(str "<li>" % "</li>") (:greetings @state)))
                    "</ul>")})
      (datastar/wrap-datastar {:rate-limit-ms 15})))

(def server (adapter/run-server handler {:port 3000}))
```

Render the datastar root element with `init-opts`. It returns the attributes
that seed the per-tab signals and open the SSE stream:

```clojure
;; in your page template
(datastar/init-opts {:selector "#greet"
                     :signals {:name "world"}
                     :anti-forgery-token csrf-token})
;; => {:data-signals "{\"name\": \"world\", 'jolt.datastar.tab-id': self.crypto.randomUUID()}"
;;     :data-init "@get(location.pathname + ...)"}
```

Datastar requests arrive with their signals parsed into
`:jolt.datastar/signals` on the request map, plus `:jolt.datastar/tab-id` and
the CSRF token bridged onto the headers. Actions can patch the client's signals
by returning `(patch-signals {...})`.

Any change to `state` now re-renders the open pages live:

```clojure
(swap! state update :greetings conj "hello")
```

## API

- `wrap-datastar`: the middleware entry point. `:rate-limit-ms` (default 15) is
  the minimum time between SSE re-renders; `:sse-param` (default
  `"datastar-sse"`) is the query parameter that marks an SSE request.
- `init-opts`: attribute map for the datastar root element. Takes `:signals`,
  `:anti-forgery-token`, and `:selector`.
- `patch-elements-event`: builds a `datastar-patch-elements` SSE event.
- `patch-signals`: a ring response that patches the client's signals.
- `signal-name` and `signals-json`: convert keywords and signal maps to the wire
  format.
- `wrap-signals`: parse signals only, without the SSE handling.

## Running the tests

```sh
jolt -M:test
```
