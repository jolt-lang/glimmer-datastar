(ns jolt.datastar.core-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [clojure.string :as str]
            [jolt.datastar.core :as ds]
            [glimmer.ratom :as ratom]
            [clojure.core.async :as async]))

;; private helpers, reached via ns-resolve so the public surface stays clean
(def parse-signal-str (ns-resolve 'jolt.datastar.core 'parse-signal-str))
(def percent-decode   (ns-resolve 'jolt.datastar.core 'percent-decode))

(defn- read-event [ch]
  (async/<!! ch))

(deftest signal-name-conversion
  (testing "wire name <-> keyword round trip"
    (is (= :foo/bar (parse-signal-str "foo_bar")))
    (is (= :foo/bar (parse-signal-str "foo.bar")))
    (is (= :jolt.datastar/tab-id (parse-signal-str "jolt.datastar.tab-id")))
    (is (= :plain (parse-signal-str "plain"))))
  (testing "keyword -> wire name"
    (is (= "foo_bar" (ds/signal-name :foo/bar)))
    (is (= "foo.bar" (ds/signal-name :foo.bar)))
    (is (= "jolt.datastar.tab-id" (ds/signal-name :jolt.datastar/tab-id)))
    (is (= "foo_a.b" (ds/signal-name [:foo/a :b])))))

(deftest signals-json-round-trip
  (let [s (ds/signals-json {:jolt.datastar/tab-id "abc" :foo/bar 1})]
    (is (str/includes? s "jolt.datastar.tab-id"))
    (is (str/includes? s "foo_bar"))))

(deftest percent-decoding
  (is (= "{\"x\":1}" (percent-decode "%7B%22x%22%3A1%7D" false)))
  (is (= "café" (percent-decode "caf%C3%A9" false)))
  (is (= "a b" (percent-decode "a+b" true)))
  (is (= "a+b" (percent-decode "a+b" false))))

(deftest patch-elements-format
  (let [e (ds/patch-elements-event "<p>hi</p>" "#greet" "inner")]
    (is (str/includes? e "event: datastar-patch-elements"))
    (is (str/includes? e "data: selector #greet"))
    (is (str/includes? e "data: mode inner"))
    (is (str/includes? e "data: namespace html"))
    (is (str/includes? e "data: elements <p>hi</p>"))
    (is (str/includes? e "data: useViewTransition false"))))

(deftest patch-signals-format
  (let [r (ds/patch-signals {:foo/bar 1})]
    (is (= 200 (:status r)))
    (is (= "application/json" (get-in r [:headers "Content-Type"])))
    (is (str/includes? (:body r) "foo_bar"))))

(deftest middleware-parses-get-signals
  (let [handler (ds/wrap-datastar (fn [req] {:status 200 :body (:jolt.datastar/signals req)}) {})]
    (is (= {:jolt.datastar/tab-id "abc"}
           (:body (handler {:request-method :get
                            :query-string "datastar=%7B%22jolt.datastar.tab-id%22%3A%22abc%22%7D"
                            :headers {"datastar-request" "true"}
                            :uri "/"}))))))

(deftest middleware-parses-json-body-signals
  (let [handler (ds/wrap-datastar (fn [req] {:status 200 :body (:jolt.datastar/signals req)}) {})]
    (is (= {:jolt.datastar/tab-id "abc", :foo/bar 1}
           (:body (handler {:request-method :post
                            :headers {"datastar-request" "true" "content-type" "application/json"}
                            :body (java.io.StringReader. "{\"jolt.datastar.tab-id\":\"abc\",\"foo_bar\":1}")
                            :uri "/"}))))))

(deftest middleware-parses-form-body-signals
  (let [handler (ds/wrap-datastar (fn [req] {:status 200 :body (:jolt.datastar/signals req)}) {})]
    (is (= {:name "café"}
           (:body (handler {:request-method :post
                            :headers {"datastar-request" "true" "content-type" "application/x-www-form-urlencoded"}
                            :body (java.io.StringReader. "datastar=%7B%22name%22%3A%22caf%C3%A9%22%7D")
                            :uri "/"}))))))

(deftest sse-streams-and-re-renders-on-ratom-change
  (let [state   (ratom/atom {:n 1})
        handler (ds/wrap-datastar (fn [_] {:status 200 :body (str "<p>" (:n @state) "</p>")}) {})
        sse     (handler {:request-method :get
                          :query-string "datastar-sse=true"
                          :headers {} :uri "/"})
        ch      (:body sse)]
    (is (= "text/event-stream; charset=utf-8" (get-in sse [:headers "Content-Type"])))
    (is (async/chan? ch))
    (let [e1 (read-event ch)]
      (is (str/includes? e1 "event: datastar-patch-elements"))
      (is (str/includes? e1 "elements <p>1</p>")))
    (swap! state assoc :n 2)
    (let [e2 (read-event ch)]
      (is (str/includes? e2 "elements <p>2</p>")))
    (async/close! ch)))

(deftest init-opts-opens-sse-stream
  (let [opts (ds/init-opts {:selector "#greet" :signals {:count 0 :name "world"}})]
    (is (str/includes? (:data-init opts) "datastar-sse=true"))
    (is (str/includes? (:data-init opts) "datastar-selector=%23greet"))
    (is (str/includes? (:data-signals opts) "jolt.datastar.tab-id"))
    (is (str/includes? (:data-signals opts) "\"count\": 0"))
    (is (str/includes? (:data-signals opts) "\"name\": \"world\""))))

(defn -main [& _]
  (let [{:keys [fail error] :as result} (run-tests 'jolt.datastar.core-test)]
    (when (pos? (+ fail error))
      (throw (ex-info "datastar tests failed" result)))))
