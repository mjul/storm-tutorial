(ns storm-tutorial.core
  (:import [backtype.storm StormSubmitter LocalCluster]
           [backtype.storm.serialization ISerialization]
           [java.io DataInputStream DataOutputStream]
           [org.joda.time DateTime])
  (:use [backtype.storm clojure config]
        [clj-time.core :only (date-time minutes plus now millis before? after?)]
        [clj-time.format :only (formatters parse unparse)]))

(defn random-step 
  "Produce a new value based on the previous value and a step size."
  [step previous]
  (+ previous (rand-nth [step (- step)])))

(defn random-walk 
  "Produce a random walk sequence beginning with the initial value and stepping up or down by the step value."
  [initial step]
  (iterate (partial random-step step) initial))

(defn price-feed
  "Produce a random walk sequence of price structures,
   beginning a time t1 with the interval in between the timestamps.
   To keep the tutorial simple, we publish mid-prices not separate value for bid and ask."
  [initial step t1 interval]
  (for [mid (random-walk initial step)
        t (iterate #(plus % interval) t1)]
    {:time t, :mid mid}))
  
;; TODO: extract generalized seq-sprout 

;; Spouts are the data sources for our tuple streams
(defspout price-feed-spout ["symbol" "t" "mid"]
  [conf context collector]
  (let [start-time (date-time 2011 10 17)
        interval (millis 100)
        sleep-time-ms (-> interval .toStandardDuration .getMillis)
        prices (atom (price-feed 1.3878 0.00001 start-time interval))]
    ;; spout is a macro that reifies the ISpout interface. It has the operations open, close, nextTuple, ack, and fail.
    (spout
     ;; nextTuple should emit the next tuple in the data stream
     (nextTuple []
                (Thread/sleep sleep-time-ms)
                (let [price (first @prices)
                      values ["EUR/USD" (:time price) (double (:mid price))]]
                  (swap! prices rest)
                  (emit-spout! collector values))))))


(defn get-sample [samples t1 t2]
  (samples [t1 t2]))

(defn merge-sample-open [t mid sample]
  (if (or (nil? (:t-open sample))
          (before? t (:t-open sample)))
    (merge sample {:t-open t, :open mid})
    sample))

(defn merge-sample-close [t mid sample]
  (if (or (nil? (:t-close sample))
          (after? t (:t-close sample)))
    (merge sample {:t-close t, :close mid})
    sample))

(defn add-sample [samples t1 t2 t sym mid]
  (let [k [t1 t2]
        after (->> (get samples k)
                   (merge-with min {:min mid})
                   (merge-with max {:max mid})
                   (merge-sample-open t mid)
                   (merge-sample-close t mid)
                   )]
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
                    sym (.getString tuple 0) 
                    t (.getValue tuple 1) 
                    mid (.getDouble tuple 2) 
                    before (get-sample @samples t1 t2)
                    after (-> (swap! samples add-sample t1 t2 t sym mid)
                              (get-sample t1 t2))]
                (if (not= before after)
                  (emit-bolt! collector [sym t1 "duration" (:min after) (:max after) "open" "close"]))
                (ack! collector tuple))))))

;; There is another way to get fields in the upcoming Storm v.0.5.4
;; (.getStringByField tuple "symbol")
;; (.getValueByField tuple "t")
;; (.getDoubleByField tuple "mid")

(defn inc-count
  "Increment the count for the symbol."
  [counts sym]
  (merge-with + counts {sym 1}))
        

;; Count the number of updates to the candlestick for each symbol
(defbolt candlestick-update-counter-bolt ["symbol" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
              (let [sym (.getString tuple 0)] 
                (swap! counts inc-count sym)
                (emit-bolt! collector [sym (get @counts sym)])
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

