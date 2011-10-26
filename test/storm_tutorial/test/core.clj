(ns storm-tutorial.test.core
  (:import [java.io DataOutputStream DataInputStream
            ByteArrayOutputStream ByteArrayInputStream]
           [backtype.storm.serialization ISerialization])
  (:use [storm-tutorial core]
        [clj-time.core :only (date-time minutes interval in-msecs)]
        [clojure.java.io]
        [clojure test]))

(deftest random-walk-tests
  (testing "should begin with the initial value"
    (is (= 1.0 (first (random-walk 1 1)))))
  (testing "should step up or down by the step factor"
    (is (#{0 2} (second (random-walk 1 1))))
    (let [actual (take 10 (random-walk 1 2))
          diffs (map - actual (rest actual))]
      (is (every? #{2 -2} diffs)))))
          
(deftest price-feed-tests
  (let [start-value 0
        step-size 10
        t (date-time 2011 1 1)
        dt (minutes 1)]
    (testing "should begin with the initial value"
      (is (= {:mid start-value, :time t}
             (first (price-feed start-value step-size t dt)))))
    (testing "time in-between should equal interval"
      (let [[p1 p2] (take 2 (price-feed start-value step-size t dt))
            t1 (:time p1)
            t2 (:time p2)]
        (is (= (interval t1 t2)) dt)))))

(deftest candlestick-sampler-tests
  (let [t1 (date-time 2011 1 1), t2 (date-time 2011 1 2), t (date-time 2011 1 1 12 00 00), v 1.0, sym "EURUSD"]
    (testing "when the set of samples is empty"
      (let [actual (add-sample {} t1 t2 t sym v)]
        (testing "should set initial value as max and min"
          (is (= v (get-in actual [[t1 t2] :max])))
          (is (= v (get-in actual [[t1 t2] :min]))))
        (testing "should set initial value as open and close"
          (is (= v (get-in actual [[t1 t2] :open])))
          (is (= v (get-in actual [[t1 t2] :close]))))))
    (testing "min/max should follow the set of observations"
      (are [v1 v2 k expected]
           (= expected
              (let [actual (-> {}
                               (add-sample t1 t2 t sym v1)
                               (add-sample t1 t2 t sym v2))]
                (get-in actual [[t1 t2] k])))
           1 1 :min 1
           1 2 :min 1
           1 0 :min 0
           1 1 :max 1
           1 2 :max 2
           1 0 :max 1))))

