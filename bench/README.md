# Benchmarks

What batching and caching buy on jolt, and why a few things are the way they
are. Numbers are from jolt v0.8.21 on a 4-core Linux machine; run the scripts
to see your own.

## Write bursts (the tick)

10 open streams, 100 `swap!`s in a row:

| | renders | frames sent |
|---|---|---|
| before (a wakeup per write) | 1000 | 55 |
| render ticks | 20 | 20 |

Before, every write queued a wakeup for every stream that read the ratom, and
each wakeup was a render, so a burst cost writes × streams renders. Now a
burst costs one render per stream per tick it spans.

## A tick's renders: `ticks.clj`

```sh
jolt -Sdeps '{:paths ["src" "bench"]}' bench/ticks.clj
```

100 streams showing a 300-row board (the rows are components) plus a line of
their own, one row changing per tick. Milliseconds per tick:

| render-threads | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| no component cache | 891 | 472 | 481 | 480 |
| component cache | 68 | 55 | 55 | 47 |

The cache is the large win: the board renders once per change, not once per
stream, and only its changed row misses. Threads scale to two and then
flatten, so `:render-threads` defaults to 2.

### Patches

With the rows as components that have ids, a tick sends each stream only the
row that changed, and a stream compares its page with the last one part by
part. The cached board is the same vector of parts on every page that shows
it, so the comparison skips it whole. Same benchmark on an M1 Max (10
cores), whole frames against patches, run alternately:

| render-threads | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| component cache, whole frames | 37.8 | 21.7 | 24.4 | 16.9 |
| component cache, patches | 9.9 | 7.0 | 5.2 | 9.8 |
| no component cache, whole frames | 395 | 213 | | |
| no component cache, patches | 460 | 253 | | |

Without the cache every row is a new string on every page, so it is compared
in full, and an id'd component is rendered into a builder of its own; that
costs about 15%. The cache is on by default.

## Render buffers: `render.clj`

```sh
jolt -Sdeps '{:paths ["src" "bench"]}' bench/render.clj
```

Hyperlith gives each render thread its own buffer, a JVM habit that avoids
regrowing a buffer on every frame. On jolt it makes no difference. A 41 KB
page, µs per render:

| | 1 thread | 4 threads (throughput) |
|---|---|---|
| per-thread builder, cut out with `substring` | 5750 | 3034 |
| fresh builder, `toString` | 5649 | 3003 |
| per-thread builder, reset, `toString` | 5630 | 2986 |

jolt's `StringBuilder` is a rope: `append` conses the piece and `toString`
joins them in one copy, so there is no buffer growth for reuse to save. The
renderer uses a fresh builder per render. A cache miss renders its component
into a builder of its own rather than cutting it out of the page's, since
`substring` flattens the rope.

The same page took 8659 µs before two other fixes. A tag and an attribute
name are each looked up in a `ConcurrentHashMap` rather than an atom. And
glimmer's `deref`, which replaces `clojure.core/deref` for the whole process,
now recognises host atoms before asking `satisfies?`. That brought a plain `@`
from 0.44 µs down to 0.17 µs.

## On the wire: `wire.clj`

```sh
jolt -Sdeps '{:paths ["src" "bench" "../ring-chez-adapter/src"]}' bench/wire.clj
```

Octets per frame through ring-chez-adapter's streaming gzip, one row changing
per frame:

| rows | raw frame | gzip each frame | gzip stream, first | gzip stream, per change | patch, raw | patch, gzip stream |
|---|---|---|---|---|---|---|
| 50 | 6503 | 899 | 920 | 92 | 255 | 28 |
| 150 | 19553 | 2389 | 2430 | 330 | 257 | 28 |
| 220 | 28862 | 3468 | 3501 | 455 | 257 | 28 |
| 300 | 39503 | 4675 | 4709 | 4316 | 257 | 35 |

While a frame fits deflate's 32 KB window, the stream matches it against the
whole previous frame, and a changed frame costs about a tenth of compressing it
on its own. Past 32 KB the stream can only match part of the previous frame,
and the saving mostly goes. Hyperlith uses zstd with a 512 KB window for this
reason. A patch (the last two columns) carries only the changed row, so its
size doesn't grow with the page and the window stops mattering. Pages
without id'd components still go whole; for those, keep patched fragments
under 32 KB, or patch more than one smaller target.
