(ns storm-tutorial.test.CustomSerializer
  (:import [storm_tutorial CustomSerializer]
           [java.io DataOutputStream DataInputStream ByteArrayOutputStream ByteArrayInputStream]
           [backtype.storm.serialization ISerialization])
  (:use [clojure test]
        [clj-time.core :only (date-time)]))

(deftest CustomSerializer-tests
  (let [sut (CustomSerializer.)]
    (testing "should implement serialization interface"
      (is (isa? (class sut) ISerialization)))
    (testing "accept"
      (testing "should accept date-time"
        (is (.accept sut org.joda.time.DateTime))))
    (testing "serialization and deserialization"
      (testing "should be reversible"
        (let [t (date-time 2010 1 2 11 22 33 444)]
          (let [bytes (with-open [out (ByteArrayOutputStream. 10000)
                                  data-out (DataOutputStream. out)]
                        (.serialize sut t data-out)
                        (.toByteArray out))
                deserialized (with-open [in (ByteArrayInputStream. bytes)
                                         data-in (DataInputStream. in)]
                               (.deserialize sut data-in))]
            (is (.equals t deserialized))))))))
