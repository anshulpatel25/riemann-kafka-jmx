(ns kafka-topics-broker.core
  (:require [zookeeper :as zk]
            [clojure.java.jmx :as jmx]
            [clojure.string :as string]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [riemann.client :as rmn])
  (:import (java.net Socket))
  (:gen-class))



(defn get-active-zkhost [ hosts zkport ]
    (doseq [ host hosts ]
    (try
      (if (Socket. host zkport)
      (def zkhost host)
      )
    (catch Exception e
      (println (str host " failed to connect, Trying another connection string"))
      ))
    ))

(defn create-zk-connection [host port]
  (def zkclient (zk/connect (string/join ":" [host port]))))

(defn get-bytes-reject [ topicname ]
 (println (str (jmx/read (str  "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=" topicname) "Count") ":" topicname ))
  )

(defn get-topics [ zkclient ]
  (zk/children zkclient "/brokers/topics"))

(defn get-partitions [ zkclient topic ]
  (zk/children zkclient (str "/brokers/topics/" topic "/partitions")))

(defn brokerid-to-host [ zkclient brokerid]
  (let [ raw (String. (:data (zk/data zkclient (str "/brokers/ids/" brokerid))) "UTF-8")
                  data (json/parse-string raw)]
                  (get data "host")))

(defn get-partition-leader [zkclient topic partition]
  (let [ raw (String. (:data (zk/data zkclient (str "/brokers/topics/" topic "/partitions/" partition "/state"  ))) "UTF-8")
                  data (json/parse-string raw)]
                  (get data "leader")))

(defn get-partiton-leader-host [ zkclient topic partition ]
  (brokerid-to-host zkclient (get-partition-leader zkclient topic partition)))

(defn get-messages-in [ jmxhost jmxport topicname ]
 (jmx/with-connection { :host jmxhost, :port jmxport}
  (jmx/read (str "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" topicname) "MeanRate")))

(defn get-bytes-in [ jmxhost jmxport topicname ]
    (jmx/with-connection { :host jmxhost, :port jmxport}
    (jmx/read (str "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=" topicname) "MeanRate")))

(defn get-bytes-out [ jmxhost jmxport topicname ]
        (jmx/with-connection { :host jmxhost, :port jmxport}
        (jmx/read (str "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=" topicname) "MeanRate")))



(defn get-replica-count [ zkclient topic partition ]
          (let [raw (String. (:data (zk/data zkclient (str "/brokers/topics/" topic ))) "UTF-8")
                data (json/parse-string  raw) ]
                (get-in data ["partitions" partition])))

(defn get-isr-count [ zkclient topic partition ]
          (let [raw (String. (:data (zk/data zkclient (str "/brokers/topics/" topic  "/partitions/" partition "/state"))) "UTF-8")
                data (json/parse-string  raw) ]
                (get data "isr")
          ))




(defn riemann-connect [ host ]
  (def rmnclnt (rmn/tcp-client {:host host})))

(defn riemann-send-event [ service host state metric description ttl ]
  (-> rmnclnt (rmn/send-event {:service service :host host :state state :metric metric :description description :ttl ttl })
  (deref 5000 ::timeout)))

(defn riemann-close [ client ]
  (rmn/close! client))

(defn -main
  [& args]
  (doseq [arg args]
  (def config  (yaml/parse-string (slurp arg)))
  (def zkport (get-in config [:zookeeper :port]))
  (get-active-zkhost (get-in config [:zookeeper :host]) zkport)
  (println zkhost)
  (def rmnhost (get-in config [:riemann :host]))
  (def rmnttl  (get-in config [:riemann :ttl]))
  )

  ;;(println zkhost)
  ;;(println rmnhost)
  (create-zk-connection zkhost zkport)
  (riemann-connect rmnhost)
  (doseq [ topic (get-topics zkclient)]
    (def  messages-in-description [ topic ])
    (def  messages-in-totalmetric 0 )

    (def bytes-in-description [ topic ])
    (def bytes-in-totalmetric 0)

    (def  topic-replica-description [ topic ])

    (def bytes-out-description [ topic ])
    (def bytes-out-totalmetric 0)

    (doseq [ partition (get-partitions zkclient topic) ]

      (let [ metric (try (get-messages-in (get-partiton-leader-host zkclient topic partition) 9999 topic) (catch Exception e -100 )) ]
      (def messages-in-description (concat messages-in-description [" Partition " partition " : Metric : "  (str metric)]))
      (def messages-in-totalmetric (+ messages-in-totalmetric metric)))


      (let [ metric (try (get-bytes-in (get-partiton-leader-host zkclient topic partition) 9999 topic) (catch Exception e -100)) ]
      (def bytes-in-description (concat bytes-in-description [" Partition " partition " : Metric : "  (str metric)]))
      (def bytes-in-totalmetric (+ bytes-in-totalmetric metric)))

      (let [ metric (try (get-bytes-out (get-partiton-leader-host zkclient topic partition) 9999 topic) (catch Exception e -100)) ]
      (def bytes-out-description (concat bytes-out-description [" Partition " partition " : Metric : "  (str metric)]))
      (def bytes-out-totalmetric (+ bytes-out-totalmetric metric)))


      (let   [replicacount (count (get-replica-count zkclient topic partition))
              isrcount     (count (get-isr-count zkclient topic partition))
              replicadata  (get-isr-count zkclient topic partition)
              isrdata      (get-isr-count zkclient topic partition)
              ]
      ;;(println topic)
      ;;(println (str "Replica:" replicacount))
      ;;(println (str "ISR:" isrcount))
        (def topic-replica-state
            (cond (< isrcount replicacount) "critical"
            :else "ok"
            )
        )

        (def topic-replica-metric
          (cond  (< isrcount replicacount) isrcount
            :else replicacount)
          )

        (def topic-replica-description (concat topic-replica-description (str " Partition : " partition " ,  Replicas : " replicadata " , ISR : " isrdata))
          )

      )


    )

  ;;    (println messages-in-description)
  ;;    (println messages-in-totalmetric)
      (let [ state
      (cond
        (or (<= messages-in-totalmetric -10 ) (>= messages-in-totalmetric 2000000000)) "critical"
        (or (<= messages-in-totalmetric -1 ) (>= messages-in-totalmetric 1000000000)) "warning"
        :else "ok"
      )]
      (riemann-send-event "kafka.topics.messages-in.MeanRate" topic state messages-in-totalmetric (apply str messages-in-description) rmnttl)
      )

    ;;  (println bytes-in-description)
    ;;  (println bytes-in-totalmetric)
    (let [ state
      (cond
        (or (<= bytes-in-totalmetric -10 ) (>= bytes-in-totalmetric 20000000000 )) "critical"
        (or (<= bytes-in-totalmetric -1 ) (>= bytes-in-totalmetric 10000000000)) "warning"
        :else "ok"
        ) ]
      (riemann-send-event "kafka.topics.bytes-in.MeanRate" topic state bytes-in-totalmetric (apply str bytes-in-description) rmnttl)
    )
    ;;  (println bytes-out-description)
    ;;  (println bytes-out-totalmetric)
     (let [ state
       (cond
         (or (<= bytes-out-totalmetric -10 ) (>= bytes-out-totalmetric 20000000000 )) "critical"
         (or (<= bytes-out-totalmetric -1 ) (>= bytes-out-totalmetric 10000000000 )) "warning"
         :else "ok"
         )]
      (riemann-send-event "kafka.topics.bytes-out.MeanRate" topic state bytes-out-totalmetric (apply str bytes-out-description) rmnttl)
  )

  (riemann-send-event "kafka.topics.replication" topic topic-replica-state topic-replica-metric (apply str topic-replica-description )  300)

  )

  ;; (riemann-send-event "kafka.topic.bytes-messages-in.MeanRate" topic "ok" totalmetric (apply str description) 300)


  (riemann-close rmnclnt)
  (zk/close zkclient))
