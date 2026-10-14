# glimmer-datastar

Datastar middleware for [jolt](https://github.com/jolt-lang/jolt). It implements
the server side of the Datastar v1.0 wire protocol: JSON signals on actions, the
`datastar-request: true` header, and `datastar-patch-*` SSE events. It parses
query strings and request bodies itself, so it needs no ring middleware stack.

## How it works

Server state lives in `glimmer.ratom` reactive cells (reagent-style atoms). Each
SSE connection renders the handler under glimmer's `*current-watcher*`, so every
ratom dereffed during the render subscribes that connection. Swap a ratom
anywhere in the app and all live pages update, reagent style.

Renders happen in batches, following Anders Murphy's [Faster streaming HTML
with batches and aggregates](https://andersmurphy.com/2026/09/29/faster-streaming-html-with-batches-and-aggregates.html):

- **Ticks.** A ratom change marks the connections that read it dirty. At most
  once per `:tick-ms` the render loop re-renders every dirty connection, once,
  however many writes landed in between, and pushes a
  `datastar-patch-elements` event when the HTML changed. A burst of writes
  costs one render per open page, not one per write per page. An idle loop
  parks.
- **Frame dropping.** Each connection's frames go out through a sliding buffer
  of one, so a client that can't keep up gets the newest frame and skips the
  rest. Nothing waits on a slow client.
- **Nothing kept for a closed page.** A connection watches exactly the ratoms
  its last render read. When the client goes away the server closes the
  stream's channel, and the connection unwatches all of them at once, so it is
  garbage even if none of them ever changes again. A render the handler
  answers with a redirect or an error, such as a 401 once the session has
  ended, ends the stream the same way.
- **Batched writes (opt-in).** `tx!` queues a thunk to run at the start of the
  next tick, before anything renders, so writes and renders take turns and no
  render sees half an update. A `:batch-fn` gets each tick's thunks together.
- **Cached components.** Render with `jolt.datastar.html` and a vector headed
  by a function, `[row item]`, is a component whose HTML is cached by function
  and arguments, and shared by every connection showing it.
- **Patches.** A component whose root element has an id, `[:tr {:id ...}]`,
  is a part of the page. When only parts changed since the last frame, the
  stream sends just those, and Datastar swaps each in by its id; one changed
  row of a 300-row board is a few hundred bytes rather than the whole board.
  When the page changed around its parts (a row added, text outside every
  part edited, an id repeated) the whole page goes as before, and so it does
  after a client missed a frame. This is the delta half of incremental view
  maintenance (DBSP's `D`): the page is still rendered whole, but only its
  change goes out. Ids have to be unique in the document. Nothing inside an
  `svg` or `math` element is a part.
- **Streaming compression.** Each frame is a whole fragment, which is cheap on
  the wire when the stream is compressed with a window carried across frames.
  `ring-chez.middleware.gzip/wrap-gzip` (ring-chez-adapter v0.7.14+) does
  this for SSE streams.

The only host dependency is Chez Scheme itself, used for percent-decoding and
hex formatting through jolt.scheme bytevector and UTF-8 primitives.

## Usage

Wrap the app handler with `wrap-datastar`, keep page state in a ratom, and run
the server:

```clojure
(require '[jolt.datastar.core :as datastar]
         '[jolt.datastar.html :as h]
         '[glimmer.ratom :as ratom]
         '[ring-chez.adapter :as adapter]
         '[ring-chez.middleware.gzip :as gzip])

(def state (ratom/atom {:greetings []}))

(defn greeting [g] [:li g])               ; a component: cached by [greeting g]

(def handler
  (-> (fn [_request]
        {:status 200
         :body (h/html [:ul (for [g (:greetings @state)] [greeting g])])})
      (datastar/wrap-datastar {:tick-ms 15})
      gzip/wrap-gzip))                      ; compresses the SSE stream too

(def server (adapter/run-server handler {:port 3000 :strategy :fibers}))
```

Render the datastar root element with `init-opts`. It returns the attributes
that seed the per-tab signals and open the SSE stream:

```clojure
;; in your page template
(datastar/init-opts {:selector "#greet"
                     :signals {:name "world"}
                     :anti-forgery-token csrf-token})
;; => {:data-signals "{\"name\": \"world\", 'jolt': {'datastar': {'tab-id': <a fresh UUID>, 'anti-forgery-token': \"...\"}}}"
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

Or, to have the write run between renders, from an action handler:

```clojure
(fn [request]
  (datastar/tx! request #(swap! state update :greetings conj "hello"))
  {:status 204})
```

`tx!` returns a promise of the thunk's result. Outside a request, make the
scheduler yourself, pass it to `wrap-datastar` as `:scheduler`, and call
`(datastar/tx! sched thunk)` with it.

## Components and the cache

`jolt.datastar.html/html` renders hiccup: `[:div#id.class {attrs} & children]`,
escaped strings (`h/raw` opts out), spliced seqs, boolean attributes, `:style`
maps as CSS and other map-valued attributes as JSON (`{:data-signals {...}}`).

A vector headed by a function, `[f a b]`, renders `(f a b)`. While
`wrap-datastar` handles a request, that HTML is cached under `[f a b]` and
shared across every connection: a board that a thousand pages show is rendered
once per change. A component has to be a function of its arguments and the
ratoms it reads. The cache records which ratoms those were and the values it
saw, and an entry stops matching as soon as one changes. On a hit the ratoms
are read again, so the page being rendered subscribes to them exactly as if
the component had run. Call `(f a b)` directly to render something uncached.

Entries are admitted on their second request, so components rendered once
don't evict ones rendered every frame, and evicted by generation within
`:max-chars` of HTML. `datastar/cache-stats` reports hits, misses and sizes.

## API

- `wrap-datastar`: the middleware entry point. Options:
  - `:tick-ms` (default 15): the minimum time between render ticks.
    `:rate-limit-ms` is still read as its old name.
  - `:render-threads` (default 2): how many threads a tick's renders are spread
    over. On jolt, renders stopped scaling past two threads on a 4-core
    machine (see `bench/README.md`).
  - `:batch-fn`: `(fn [thunks])` applying each tick's `tx!` thunks.
  - `:cache`: options for the component cache (`:max-chars`, `:doorkeeper`), or
    `false` for none.
  - `:scheduler`: a scheduler from `scheduler` to use instead of a new one.
  - `:sse-param` (default `"datastar-sse"`): the query parameter that marks an
    SSE request.
  - `:component-patches` (default true): send only the changed parts of a
    page, see Patches above. `false` sends every frame whole.
- `scheduler`, `stop!`: make (and stop) a render loop to share between
  `wrap-datastar` and code outside requests.
- `tx!`: queue a write for the start of the next tick; takes the request or a
  scheduler.
- `cache-stats`: the component cache's counters.
- `init-opts`: attribute map for the datastar root element. Takes `:signals`,
  `:anti-forgery-token`, and `:selector`.
- `patch-elements-event`: builds a `datastar-patch-elements` SSE event. A nil
  selector with mode `outer` replaces elements by their ids.
- `patch-signals`: a ring response that patches the client's signals.
- `signal-name` and `signals-json`: convert keywords and signal maps to the wire
  format. `:foo/bar` is `foo_bar`. A dotted namespace is a path on the client,
  so `signals-json` writes `:a.b/c` as `{"a": {"b": {"c": ...}}}`, since the
  datastar client drops a JSON key with a dot in it, and the client sends it
  back nested the same way, which parses to `{:a {:b {:c ...}}}`.
- `wrap-signals`: parse signals only, without the SSE handling.

## Running the tests

```sh
jolt -M:test
```

## License

MIT (see `LICENSE`).
