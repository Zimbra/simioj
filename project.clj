(defproject simioj "0.1.0-SNAPSHOT"
  :description "Simioj - Asynchronous Task Framework"
  :url "http://zimbra.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cheshire "5.6.1"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/data.json "0.2.6"]
                 [ring/ring-core "1.4.0"]
                 [ring/ring-jetty-adapter "1.4.0"]
                 [compojure "1.5.0"]
                 [ring/ring-json "0.4.0"]
                 [clj-http "1.1.2"]
                 [clj-http-fake "1.0.2"]
                 [log4j/log4j "1.2.17"]
                 [org.apache.commons/commons-daemon "1.0.9"]
                 [org.clojure/core.async "0.2.374"]
                 ;; the following two entries are to support sqlite
                 [org.clojure/java.jdbc "0.3.7"]
                 [org.xerial/sqlite-jdbc "3.8.9"]]
  :main ^:skip-aot zimbra.simioj.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[ring/ring-mock "0.3.0"]
                                  [clj-http-fake "1.0.2"]
                                  [org.clojure/tools.namespace "0.2.11"]]
                   :plugins [[lein-kibit "0.1.2"]]}}
  :aot :all)
