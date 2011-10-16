(ns storm-tutorial.test.core
  (:import [java.io DataOutputStream DataInputStream
            ByteArrayOutputStream ByteArrayInputStream]
           [backtype.storm.serialization ISerialization])
  (:use [storm-tutorial core]
        [clj-time.core :only (date-time)]
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
          

(deftest interval-sampler-tests
  (let [t1 :t1, t2 :t2, v 1.0, cross "EURUSD"]
    (testing "when the set of samples is empty"
      (testing "should set initial value as max and min"
        (is (=  {[t1 t2] {:min v, :max v}}
                (add-sample {} t1 t2 cross v)))))
    (testing "min/max should follow the set of observations"
      (are [v1 v2 k expected]
           (= expected
              (let [actual (-> {}
                               (add-sample t1 t2 cross v1)
                               (add-sample t1 t2 cross v2))]
                (get-in actual [[t1 t2] k])))
           1 1 :min 1
           1 2 :min 1
           1 0 :min 0
           1 1 :max 1
           1 2 :max 2
           1 0 :max 1))))



