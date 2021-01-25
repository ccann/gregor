# Gregor

Lightweight Clojure wrapper for [Apache Kafka](http://kafka.apache.org/) Consumer + Producer APIs.

```clojure
 :dependencies [[org.clojure/clojure "1.10.0"]
                [org.apache.kafka/kafka_2.12 "2.1.1"]]

```
[![cljdoc Badge](https://cljdoc.org/badge/io.weft/gregor)](https://cljdoc.org/d/io.weft/gregor/CURRENT)

[![Clojars Project](https://clojars.org/io.weft/gregor/latest-version.svg)](https://clojars.org/io.weft/gregor)

[**CHANGELOG**](https://github.com/ccann/gregor/blob/master/CHANGELOG.md)

Gregor wraps most of the Java API for the Kafka [Producer](http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) and [New Consumer](http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) The goal of this project is to stay very close to the Kafka API instead of adding more advanced features.

## Example

Here's an example of at-least-once processing (using [`mount`](https://github.com/tolitius/mount)):

```clojure
(ns gregor-sample-app.core
  (:gen-class)
  (:require [clojure.repl :as repl]
            [gregor.core :as gregor]
            [mount.core :as mount :refer [defstate]]))

(def run (atom true))

(defstate consumer
  :start (gregor/consumer "localhost:9092"
                          "testgroup"
                          ["test-topic"]
                          {"auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"})
  :stop (gregor/close consumer))

(defstate producer
  :start (gregor/producer "localhost:9092")
  :stop (gregor/close producer))

(defn -main
  [& args]
  (mount/start)
  (repl/set-break-handler! (fn [sig] (reset! run false)))
  (while @run
    (let [consumer-records (gregor/poll consumer)
          values (process-records consumer-records)]
      (doseq [v values]
        (gregor/send producer "other-topic" v))
      (gregor/commit-offsets! consumer)))
  (mount/stop))
```

Transformations over consumer records are applied in `process-records`. Each record in
the `seq` returned by `poll` is a map. Here's an example with a JSON object as the
`:value`:

```clojure
{:value "{\"foo\":42}"
 :key nil
 :partition 0
 :topic "test-topic"
 :offset 939}
```

## Producing

Gregor provides the `send` function for asynchronously sending a record to a topic. There
are multiple arities which correspond to those of the `ProducerRecord`
[Java constructor](https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html). If
you'd like to provide a callback to be invoked when the send has been acknowledged use
`send-then` instead.

## Topic Management

Create an admin client

```clojure
(def admin-client (admin "localhost:9092"))
```

Create a topic:

```clojure
(create-topic admin-client "some-topic" {})
```
That empty map can be used to specify configuration for number of topic partitions, replication factor,

Delete a topic:

``` clojure
(delete-topic admin-client "some-topic")
```

Query about a topic's existence:

``` clojure
(topic-exists? admin-client "some-topic")
```

List existing topics:

``` clojure
(topics admin-client)
```

## License

Distributed under the Eclipse Public License either version 1.0 or (at your option) any
later version.
