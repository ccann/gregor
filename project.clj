(defproject io.weft/gregor "1.0.1-SNAPSHOT"
  :min-lein-version "2.0.0"
  :description "Lightweight Clojure bindings for Kafka"
  :url "https://github.com/ccann/gregor.git"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.apache.kafka/kafka_2.12 "2.4.1"]]
  :plugins [[lein-eftest "0.5.6"]]
  :deploy-repositories {"clojars" {:url           "https://clojars.org/repo"
                                   :sign-releases false
                                   :username      :env
                                   :passphrase    :env}}
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["uberjar"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy" "clojars"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push" "--no-verify"]])
