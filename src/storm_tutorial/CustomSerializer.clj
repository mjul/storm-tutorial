(ns storm-tutorial.CustomSerializer
  (:import [backtype.storm StormSubmitter LocalCluster]
           [backtype.storm.serialization ISerialization]
           [java.io DataInputStream DataOutputStream]
           [org.joda.time DateTime])
  (:use [backtype.storm clojure config]
        [clj-time.core :only (date-time now)]
        [clj-time.format :only (formatters parse unparse)])
  (:gen-class
   :implements [backtype.storm.serialization.ISerialization]))

;; Configure using this with the config map:  {TOPOLOGY-SERIALIZATIONS {100 (.getName storm_tutorial.CustomSerializer)}}

(def ISO-DATE-TIME-FORMATTER (formatters :date-time))

(defn- serialize-date-time [^DateTime dt]
  (-> (unparse ISO-DATE-TIME-FORMATTER dt)
      .getBytes))

(def DATE-TIME-LENGTH (count (serialize-date-time (now))))

(defn -init []
  [[] nil])

(defn ^boolean -accept [this, ^Class c]
  (= c org.joda.time.DateTime))

(defn -serialize [this, ^Object obj, ^DataOutputStream stream]
  (.write stream (serialize-date-time (cast DateTime obj))))

(defn ^Object -deserialize [this, ^DataInputStream stream]
  (let [bytes (byte-array DATE-TIME-LENGTH)]
    (.read stream bytes 0 DATE-TIME-LENGTH)
    (parse (String. bytes))))

;; TODO: (defmulti serialize class [dt])
;; TODO: (defmulti deserialize class [dt])

