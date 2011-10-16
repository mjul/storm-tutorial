(ns storm-tutorial.core
  (:import [backtype.storm StormSubmitter LocalCluster]
           [backtype.storm.serialization ISerialization]
           [java.io DataInputStream DataOutputStream]
           [org.joda.time DateTime])
  (:use [backtype.storm clojure config]
        [clj-time.core :only (date-time minutes plus now)]
        [clj-time.format :only (formatters parse unparse)]))

(defn random-step 
  "Produce a new value based on the previous value and a step size."
  [step previous]
  (+ previous (rand-nth [step (- step)])))

(defn random-walk 
  "Produce a random walk sequence beginning with the initial value and stepping up or down by the step value."
  [initial step]
  (iterate (partial random-step step) initial))

;; TODO: quote-stream (bid/ask)

;; Spouts are the data sources for our tuple streams
(defspout price-feed-spout ["symbol" "mid"]
  [conf context collector]
  (let [prices (atom (random-walk 1.3878 0.00001))]
    ;; spout is a macro that reifies the ISpout interface. It has the operations open, close, nextTuple, ack, and fail.
    (spout
     ;; nextTuple should emit the next tuple in the data stream
     (nextTuple []
                (Thread/sleep 100)
                (let [price (first @prices)
                      values ["EUR/USD" (double price)]]
                  (swap! prices rest)
                  (emit-spout! collector values))))))


(defn get-sample [samples t1 t2]
  (samples [t1 t2]))

(defn add-sample [samples t1 t2 symbol mid]
  (let [k [t1 t2]
        after (->> (get samples k)
                   (merge-with min {:min mid})
                   (merge-with max {:max mid}))]
    (assoc samples k after)))


;; Bolts are stream processing functions
;; TODO: make it parametrized on the duration
(defbolt candlestick-sampler-bolt ["symbol" "t1" "duration" "min" "max" "open" "close"] {:prepare true}
  [conf context collector]
  (let [samples (atom {})
        duration (minutes 1)]
    (bolt
     (execute [tuple]
              (let [t1 (date-time 2011 10 14)
                    t2 (plus t1 duration)
                    symbol (.getString tuple 0)
                    mid (.getDouble tuple 1)
                    before (get-sample @samples t1 t2)
                    after (-> (swap! samples add-sample t1 t2 symbol mid)
                              (get-sample t1 t2))]
                (if (not= before after)
                  (emit-bolt! collector [symbol t1 "duration" (:min after) (:max after) "open" "close"]))
                (ack! collector tuple))))))

(defn inc-count
  "Increment the count for the symbol."
  [counts symbol]
  (merge-with + counts {symbol 1}))
        

;; Count the number of updates to the candlestick for each symbol
(defbolt candlestick-update-counter-bolt ["symbol" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
              (let [symbol (.getString tuple 0)]
                (swap! counts inc-count symbol)
                (emit-bolt! collector [symbol (get @counts symbol)])
                (ack! collector tuple))))))

(defn mk-topology []
  (topology
   {1 (spout-spec price-feed-spout)}
   {2 (bolt-spec {1 :shuffle}
                 candlestick-sampler-bolt)
    3 (bolt-spec {2 ["symbol"]}
                 ;; If the grouping specification is a seq it specifies a fields grouping.
                 ;; Here we distribute the tuples to the bolt instances based on the value of the "symbol" field.
                  candlestick-update-counter-bolt)}
   ))


(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "storm-tutorial"
                     {TOPOLOGY-DEBUG true,
                      TOPOLOGY-SERIALIZATIONS {100 (.getName storm_tutorial.CustomSerializer)}}
                     (mk-topology))
    (Thread/sleep 5000)
    (.shutdown cluster)
    ))


;; TODO: seq-sprout