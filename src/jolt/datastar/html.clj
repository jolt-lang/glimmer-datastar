(ns jolt.datastar.html
  "Hiccup to HTML, with cached components.

  The usual hiccup forms render as you would expect:

    [:div#main.card {:class [\"wide\"] :style {:color \"red\"}} \"text\" child]

  Strings are escaped (`raw` opts out), nil and false render nothing, seqs
  splice, a boolean true attribute renders bare, a map-valued :style becomes
  CSS and any other map-valued attribute becomes JSON (so :data-signals can be
  given a map).

  A vector whose head is a function is a component: [f a b] renders whatever
  (f a b) returns. While a component cache is bound (wrap-datastar binds one
  for every request it handles) its HTML is kept keyed by the vector itself, f
  and its arguments, and shared between every page that renders it. A
  component must be a function of its arguments and of the ratoms it reads:
  the cache records which ratoms those were and the values it saw, and an
  entry stops matching as soon as one of them changes. On a hit the ratoms are
  read again, so the page rendering it subscribes to them just as though the
  component had run. Calling (f a b) directly instead renders it uncached.

  A component whose root is one element with an id is a part of the page:
  html-parts returns the page with each part's id, HTML and offset, which is
  what lets a stream send the parts that changed instead of the whole page.
  A component without an id is transparent, and its own parts are the page's.
  Nothing inside an svg or math element is a part, since a patch is parsed as
  HTML."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [glimmer.ratom :as ratom]
            [jolt.datastar.cache :as cache]))

(def ^:dynamic *cache*
  "The component cache in effect, or nil to render components uncached."
  nil)

;; The parts of the builder being written, a list of [id html offset], or nil.
(def ^:dynamic ^:private *parts* nil)

;; True inside svg or math, where a part could not be patched as HTML.
(def ^:dynamic ^:private *foreign* false)

(def ^:dynamic *page*
  "Bound to a volatile to be told the last page html rendered, as
  [html groups]: groups are [offset parts component-html], one per component
  written that has parts, each part [id its-html offset-in-component-html]. A
  stream binds it around the handler to find the parts of the body it
  returns."
  nil)

(deftype Raw [s])

(defn raw
  "Mark s as HTML to be written as is, without escaping."
  [s]
  (->Raw (str s)))

(def ^:private void-tags
  #{"area" "base" "br" "col" "embed" "hr" "img" "input" "link" "meta" "param"
    "source" "track" "wbr"})

(defn- escape
  "s with the five HTML-significant characters escaped. Most text has none,
  so look before building a new string."
  [^String s]
  (if (or (str/index-of s "&") (str/index-of s "<") (str/index-of s ">")
          (str/index-of s "\"") (str/index-of s "'"))
    (-> s
        (str/replace "&" "&amp;")
        (str/replace "<" "&lt;")
        (str/replace ">" "&gt;")
        (str/replace "\"" "&quot;")
        (str/replace "'" "&#39;"))
    s))

(defn- as-str [x]
  (if (keyword? x) (name x) (str x)))

;; --- tags --------------------------------------------------------------------
;; A tag keyword is parsed once: :div#main.a.b -> ["div" "main" "a b"]. Tags
;; are a small, fixed set, so the cache is unbounded. A ConcurrentHashMap
;; rather than an atom: with glimmer loaded every @ goes through its dispatching
;; deref, and this lookup happens for every element.

(def ^:private ^java.util.concurrent.ConcurrentHashMap tag-cache
  (java.util.concurrent.ConcurrentHashMap.))

(defn- parse-tag [tag]
  (or (.get tag-cache tag)
      (let [s       (name tag)
            [_ nm rest] (re-matches #"([^#.]+)(.*)" s)
            id      (second (re-find #"#([^.]+)" (or rest "")))
            classes (map second (re-seq #"\.([^#.]+)" (or rest "")))
            parsed  [nm id (when (seq classes) (str/join " " classes))]]
        (.put tag-cache tag parsed)
        parsed)))

;; --- attributes --------------------------------------------------------------

(defn- write-style! [^StringBuilder sb m]
  (loop [es (seq m) first? true]
    (when es
      (let [[k v] (first es)]
        (if (some? v)
          (do (when-not first? (.append sb ";"))
              (.append sb ^String (as-str k))
              (.append sb ":")
              (.append sb ^String (escape (as-str v)))
              (recur (next es) false))
          (recur (next es) first?))))))

(defn- class-str [v]
  (cond
    (string? v) v
    (keyword? v) (name v)
    (coll? v) (str/join " " (keep #(when % (as-str %)) v))
    :else (str v)))

;; " name=\"" for each attribute keyword, built once. Attribute names, like
;; tags, are a small fixed set.
(def ^:private ^java.util.concurrent.ConcurrentHashMap attr-prefix
  (java.util.concurrent.ConcurrentHashMap.))

(defn- prefix ^String [k]
  (or (.get attr-prefix k)
      (let [p (str " " (as-str k) "=\"")]
        (.put attr-prefix k p)
        p)))

(defn- write-attr! [^StringBuilder sb k v]
  (cond
    (or (nil? v) (false? v)) nil
    (true? v) (do (.append sb " ") (.append sb ^String (as-str k)))
    (string? v) (do (.append sb (prefix k))
                    (.append sb ^String (escape v))
                    (.append sb "\""))
    (and (identical? k :style) (map? v))
    (do (.append sb (prefix k))
        (write-style! sb v)
        (.append sb "\""))
    :else
    (let [s (cond
              (identical? k :class) (class-str v)
              (map? v) (json/write-str v)
              (coll? v) (str/join " " (map as-str v))
              :else (as-str v))]
      (.append sb (prefix k))
      (.append sb ^String (escape s))
      (.append sb "\""))))

(defn- write-attrs!
  "id and class first (from the tag shorthand, merged with the map), then the
  rest in map order."
  [sb id tag-classes attrs]
  (let [id    (if (contains? attrs :id) (:id attrs) id)
        class (let [c (:class attrs)]
                (cond
                  (and tag-classes (some? c) (not (false? c))) (str tag-classes " " (class-str c))
                  tag-classes tag-classes
                  :else c))]
    (write-attr! sb :id id)
    (write-attr! sb :class class)
    (doseq [[k v] attrs
            :when (not (or (identical? k :id) (identical? k :class)))]
      (write-attr! sb k v))))

;; --- nodes ---------------------------------------------------------------------

(declare write-node!)

(defn- write-element! [^StringBuilder sb [tag & more]]
  (let [[nm id tag-classes] (parse-tag tag)
        attrs (when (map? (first more)) (first more))
        children (if attrs (rest more) more)]
    (.append sb "<")
    (.append sb nm)
    (write-attrs! sb id tag-classes attrs)
    (.append sb ">")
    (when-not (contains? void-tags nm)
      (if (or (= "svg" nm) (= "math" nm))
        (binding [*foreign* true] (doseq [c children] (write-node! sb c)))
        (doseq [c children] (write-node! sb c)))
      (.append sb "</")
      (.append sb nm)
      (.append sb ">"))))

(defn- call-component [form]
  (apply (first form) (rest form)))

(defn- current? [cells vals]
  (loop [cs (seq cells) vs (seq vals)]
    (if cs
      (let [now (ratom/-value (first cs))
            was (first vs)]
        (if (or (identical? now was)
                ;; a scalar recomputed to an equal value (a boxed number, a
                ;; string rebuilt by str) is still the same input
                (and (not (coll? now)) (= now was)))
          (recur (next cs) (next vs))
          false))
      true)))

(defn- root-id
  "The id of node when it is one element with an id, else nil."
  [node]
  (when (and (vector? node) (keyword? (first node)))
    (let [attrs (second node)]
      (if (and (map? attrs) (contains? attrs :id))
        (some-> (:id attrs) as-str)
        (second (parse-tag (first node)))))))

;; Parts are kept in groups, [offset parts html], one per component written:
;; the component's HTML, and its parts with offsets into that HTML. A cached
;; component brings its parts as one group, the vector stored with it, rather
;; than a new vector per part on every hit, and a group that is the identical
;; vector a page had last time needs no further look.

(defn flatten-groups
  "Part groups, as *page* is given them, as one vector of [id html offset]."
  [groups]
  (into [] (mapcat (fn [[off parts]]
                     (map (fn [[id h start]] [id h (+ off start)]) parts)))
        groups))

(defn- emit!
  "Append html, and its parts as a group of the builder's."
  [^StringBuilder sb ^String html parts]
  (let [off (.length sb)]
    (.append sb html)
    (when-let [^java.util.ArrayList ps *parts*]
      (when (and (seq parts) (not *foreign*))
        (.add ps [off parts html])))))

(defn- render-component
  "Render form into {:html :parts}, its parts relative to its own HTML: the
  component itself when it has an id, else the parts within."
  [form]
  (let [inner (StringBuilder.)
        ps    (java.util.ArrayList.)
        node  (call-component form)]
    (binding [*parts* ps *foreign* false]
      (write-node! inner node))
    (let [html (.toString inner)]
      {:html  html
       :parts (if-let [id (root-id node)] [[id html 0]] (flatten-groups ps))})))

(defn- write-component! [^StringBuilder sb form]
  (if-let [c *cache*]
    (let [e (cache/lookup c form)]
      (if (and e (current? (:cells e) (:vals e)))
        (do (cache/record! c :hits)
            ;; re-read the cells: subscribes this render (and any capture
            ;; around it, for an enclosing cached component) to them
            (run! ratom/deref (:cells e))
            (emit! sb (:html e) (:parts e)))
        ;; rendered into a builder of its own: cutting it out of the outer
        ;; one with substring would make jolt's StringBuilder (a rope) flatten
        ;; everything rendered so far
        (let [[r cells] (ratom/capture #(render-component form))]
          (emit! sb (:html r) (:parts r))
          (cache/record! c (if e :stale :misses))
          (cache/admit! c form (assoc r
                                      :cells cells
                                      :vals  (mapv ratom/-value cells))))))
    (let [node (call-component form)]
      (if-let [id (root-id node)]
        (let [inner (StringBuilder.)
              ^java.util.ArrayList ps *parts*
              n (when ps (.size ps))]
          (write-node! inner node)
          ;; parts within it are its own, and their offsets are in inner:
          ;; drop them (cheaper than a binding, which costs every component)
          (when ps
            (while (> (.size ps) n) (.remove ps (int (dec (.size ps))))))
          (let [html (.toString inner)]
            (emit! sb html [[id html 0]])))
        ;; no id: write in place, and its parts are the builder's own
        (write-node! sb node)))))

(defn- write-node! [^StringBuilder sb node]
  (cond
    (nil? node) nil
    (string? node) (.append sb ^String (escape node))
    (instance? Raw node) (.append sb ^String (.-s ^Raw node))
    (vector? node) (let [head (first node)]
                     (cond
                       (keyword? head) (write-element! sb node)
                       (fn? head) (write-component! sb node)
                       :else (doseq [c node] (write-node! sb c))))
    (sequential? node) (doseq [c node] (write-node! sb c))
    (false? node) nil
    :else (.append sb ^String (escape (as-str node)))))

;; --- rendering ---------------------------------------------------------------
;; A fresh StringBuilder per render. Keeping one per thread and reusing it, the
;; JVM habit (hyperlith keeps a direct ByteBuffer per render thread), measured
;; the same here to within 1%: jolt's StringBuilder appends by consing the
;; piece and joins once at toString, so there is no buffer growth for reuse to
;; save (bench/render.clj).

(defn- render-page
  "[html groups] for hiccup nodes, also given to *page* when it is bound."
  [nodes]
  (let [sb (StringBuilder.)
        ps (java.util.ArrayList.)]
    (binding [*parts* ps *foreign* false]
      (doseq [n nodes] (write-node! sb n)))
    (let [r [(.toString sb) (vec ps)]]
      (when-let [p *page*] (vreset! p r))
      r)))

(defn html-parts
  "Render hiccup nodes to [html parts]: the HTML string, and for each
  component in it whose root is an element with an id, [id its-html offset],
  in page order."
  [& nodes]
  (let [[s groups] (render-page nodes)]
    [s (flatten-groups groups)]))

(defn html
  "Render hiccup nodes to an HTML string."
  [& nodes]
  (first (render-page nodes)))
