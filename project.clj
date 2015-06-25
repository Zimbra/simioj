(defproject simioj "0.1.0-SNAPSHOT"
  :description "Simioj - Asynchronous Task Framework"
  :url "http://zimbra.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/data.json "0.2.6"]
                 [ring/ring-core "1.3.2"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [compojure "1.3.2"]
                 [ring/ring-json "0.3.1"]
                 [clj-http "1.0.1"]
                 [clj-http-fake "1.0.1"]
                 [log4j/log4j "1.2.17"]
                 [org.apache.commons/commons-daemon "1.0.9"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 ;; the following two entries are to support sqlite
                 [org.clojure/java.jdbc "0.3.7"]
                 [org.xerial/sqlite-jdbc "3.8.9"]]
  :main ^:skip-aot zimbra.simioj.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[ring/ring-mock "0.2.0"]
                                  [clj-http-fake "1.0.1"]]
                   :plugins [[lein-kibit "0.0.8"]]}}
  :aot :all)
