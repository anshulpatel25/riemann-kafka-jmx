(defproject kafka-topics-broker "0.2.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [zookeeper-clj "0.9.4"]
                 [org.clojure/java.jmx "0.2.0"]
                 [cheshire "5.6.1"]
                 [riemann-clojure-client "0.4.2"]
                 [clj-yaml "0.4.0"]]
  :main ^:skip-aot kafka-topics-broker.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
