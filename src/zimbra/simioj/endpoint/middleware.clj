(ns zimbra.simioj.endpoint.middleware
  (:require
   [cheshire.core :as json]
   [clojure.string :refer [split]]
   [clojure.tools.logging :as logger]
   [ring.util.response :refer [content-type]])
  (:gen-class))


(defn accept-header [request & {:keys [default] :or {default "*/*"}}]
  (first (split ((:headers request) "accept" default) #";")))


(defn wrap-response
  "Middleware that converts response to either EDN or JSON, based on the
  value Accept header, as follows:
  - application/json - converts using wrap-json-response.  See that
    method for available options (:pretty and :escape-non-ascii)
  - application/edn, */*, or no Accept header - converts to EDN.
  "
  [handler & [{:as options}]]
  (fn [request]
    (let [accepts (accept-header request :default "application/edn")
          response (handler request)
          raw-body (:body response)
          [body ct] (condp = accepts
                      "application/json" [(json/generate-string raw-body options)
                                          "application/json; charset=utf-8"]
                      [(pr-str raw-body) "application/edn; charset=utf-8"])
          encoded-response (assoc response :body body)]
      (content-type encoded-response ct))))
