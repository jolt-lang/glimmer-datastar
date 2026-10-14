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
  component had run. Calling (f a b) directly instead renders it uncached."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [glimmer.ratom :as ratom]
            [jolt.datastar.cache :as cache]))

(def ^:dynamic *cache*
  "The component cache in effect, or nil to render components uncached."
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
      (doseq [c children] (write-node! sb c))
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

(defn- write-component! [^StringBuilder sb form]
  (if-let [c *cache*]
    (let [e (cache/lookup c form)]
      (if (and e (current? (:cells e) (:vals e)))
        (do (cache/record! c :hits)
            ;; re-read the cells: subscribes this render (and any capture
            ;; around it, for an enclosing cached component) to them
            (run! ratom/deref (:cells e))
            (.append sb ^String (:html e)))
        ;; rendered into a builder of its own: cutting it out of the outer
        ;; one with substring would make jolt's StringBuilder (a rope) flatten
        ;; everything rendered so far
        (let [inner (StringBuilder.)
              [_ cells] (ratom/capture #(write-node! inner (call-component form)))
              html (.toString inner)]
          (.append sb html)
          (cache/record! c (if e :stale :misses))
          (cache/admit! c form {:html  html
                                :cells cells
                                :vals  (mapv ratom/-value cells)}))))
    (write-node! sb (call-component form))))

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

(defn html
  "Render hiccup nodes to an HTML string."
  [& nodes]
  (let [sb (StringBuilder.)]
    (doseq [n nodes] (write-node! sb n))
    (.toString sb)))
