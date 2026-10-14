(ns jolt.datastar.html-test
  (:require [clojure.test :refer [deftest is testing]]
            [glimmer.ratom :as ratom]
            [jolt.datastar.cache :as cache]
            [jolt.datastar.html :as h]))

(deftest renders-hiccup
  (testing "elements, attributes and text"
    (is (= "<div id=\"a\" class=\"x y\"><p>hi</p></div>"
           (h/html [:div {:id "a" :class "x y"} [:p "hi"]]))))
  (testing "id and class shorthand merge with the attribute map"
    (is (= "<div id=\"main\" class=\"card wide\">x</div>"
           (h/html [:div#main.card {:class ["wide"]} "x"]))))
  (testing "text and attribute values are escaped; raw is not"
    (is (= "<p title=\"&quot;q&quot;\">&lt;b&gt; &amp; &#39;</p><b>ok</b>"
           (h/html [:p {:title "\"q\""} "<b> & '"] (h/raw "<b>ok</b>")))))
  (testing "nil, false and boolean attributes"
    (is (= "<input disabled>" (h/html [:input {:disabled true :checked false :value nil}]))))
  (testing "void tags have no close tag"
    (is (= "<br><img src=\"a.png\">" (h/html [:br] [:img {:src "a.png"}]))))
  (testing "seqs splice, numbers and keywords render as text"
    (is (= "<ul><li>1</li><li>2</li></ul>k"
           (h/html [:ul (for [i [1 2]] [:li i])] nil false :k))))
  (testing "style maps become CSS, other maps JSON"
    (is (= "<div style=\"color:red;margin:0\" data-signals=\"{&quot;n&quot;:1}\"></div>"
           (h/html [:div {:style {:color "red" :margin 0} :data-signals {:n 1}}])))))

(deftest components
  (let [greet (fn [n] [:b "hi " n])]
    (testing "a vector headed by a fn renders what it returns"
      (is (= "<p><b>hi x</b></p>" (h/html [:p [greet "x"]]))))
    (testing "html can be called inside a render"
      (is (= "<p><i>a</i>b</p>" (h/html [:p (h/raw (h/html [:i "a"])) "b"]))))))

(defn- counting [f]
  (let [n (atom 0)]
    [n (fn [& args] (swap! n inc) (apply f args))]))

(deftest cache-admits-on-the-second-request-and-shares
  (let [c (cache/new-cache)
        [calls row] (counting (fn [i] [:li i]))
        render #(binding [h/*cache* c] (h/html [:ul [row 1] [row 2]]))]
    (is (= "<ul><li>1</li><li>2</li></ul>" (render)))
    (is (= 2 @calls) "first sight: rendered, only noted")
    (render)
    (is (= 4 @calls) "second sight: rendered and admitted")
    (is (= "<ul><li>1</li><li>2</li></ul>" (render)))
    (is (= 4 @calls) "third: served from the cache")
    (is (= 2 (:hits (cache/stats c))))))

(deftest cache-entries-follow-the-ratoms-they-read
  (let [c (cache/new-cache)
        state (ratom/atom {:n 1})
        [calls counter] (counting (fn [label] [:span label (:n @state)]))
        render #(binding [h/*cache* c] (h/html [counter "n="]))]
    (render) (render)
    (is (= "<span>n=1</span>" (render)))
    (is (= 2 @calls))
    (swap! state assoc :n 2)
    (is (= "<span>n=2</span>" (render)) "a changed ratom makes the entry stale")
    (is (= 3 @calls))
    (is (= "<span>n=2</span>" (render)))
    (is (= 3 @calls) "and the refreshed entry is served again")
    (is (= 1 (:stale (cache/stats c))))))

(deftest a-cache-hit-still-subscribes-the-render
  ;; the friendly part: a page served a cached component must still hear about
  ;; the ratoms that component reads, or it would stop updating
  (let [c (cache/new-cache)
        state (ratom/atom 1)
        comp (fn [] [:i @state])
        render (fn [watcher]
                 (binding [h/*cache* c ratom/*current-watcher* watcher]
                   (h/html [comp])))
        fired (atom [])]
    (render (fn [_] nil))
    (render (fn [_] nil))                       ; now cached
    (render (fn [_] (swap! fired conj :page)))  ; a hit, for a new page
    (reset! state 2)
    (is (= [:page] @fired))))

(deftest nested-cached-components
  (let [c (cache/new-cache)
        inner-state (ratom/atom "a")
        inner (fn [] [:i @inner-state])
        outer (fn [] [:p [inner]])
        render #(binding [h/*cache* c] (h/html [outer]))]
    (dotimes [_ 3] (render))
    (is (= "<p><i>a</i></p>" (render)))
    (reset! inner-state "b")
    ;; the outer entry recorded the inner one's ratom, so it goes stale too
    (is (= "<p><i>b</i></p>" (render)))))

(deftest uncached-without-a-cache
  (let [[calls row] (counting (fn [] [:br]))]
    (dotimes [_ 3] (h/html [row]))
    (is (= 3 @calls))))

(defn- part-ids [[_ parts]] (mapv first parts))

(deftest parts-are-the-components-with-ids
  (let [row (fn [i] [:tr {:id (str "r" i)} [:td i]])
        [s parts :as r] (h/html-parts [:table#t [row 1] [row 2]])]
    (is (= "<table id=\"t\"><tr id=\"r1\"><td>1</td></tr><tr id=\"r2\"><td>2</td></tr></table>" s))
    (is (= ["r1" "r2"] (part-ids r)))
    (testing "each part is its HTML at its offset in the page"
      (doseq [[_ html start] parts]
        (is (= html (subs s start (+ start (count html)))))))
    (testing "id shorthand on the tag counts too"
      (is (= ["x"] (part-ids (h/html-parts [(fn [] [:div#x "a"])])))))))

(deftest a-component-without-an-id-is-transparent
  (let [row (fn [i] [:li {:id (str "i" i)} i])
        rows (fn [] [:ul (for [i [1 2]] [row i])])]
    (is (= ["i1" "i2"] (part-ids (h/html-parts [:div [rows]]))))
    (testing "and stays so when it comes from the cache"
      (let [c (cache/new-cache)
            render #(binding [h/*cache* c] (h/html-parts [:div [rows]]))]
        (dotimes [_ 2] (render))
        (let [[s parts :as r] (render)]
          (is (pos? (:hits (cache/stats c))))
          (is (= ["i1" "i2"] (part-ids r)))
          (doseq [[_ html start] parts]
            (is (= html (subs s start (+ start (count html)))))))))))

(deftest a-part-holds-its-own-components
  (let [cell (fn [i] [:td {:id (str "c" i)} i])
        row (fn [] [:tr#r (for [i [1 2]] [cell i])])]
    (is (= ["r"] (part-ids (h/html-parts [:table [row]]))))))

(deftest no-parts-inside-svg-or-math
  ;; a patch is parsed as HTML, which would make an SVG child an HTML element
  (let [dot (fn [i] [:circle {:id (str "d" i) :r i}])]
    (is (= [] (part-ids (h/html-parts [:svg#s [dot 1]]))))
    (is (= ["s"] (part-ids (h/html-parts [(fn [] [:svg#s [dot 1]])]))))))
