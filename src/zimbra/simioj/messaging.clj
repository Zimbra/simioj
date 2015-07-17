(ns zimbra.simioj.messaging
  (:gen-class)
  (:require [zimbra.simioj.config :as config]
            [clojure.tools.logging :as log]
            [clj-http.client :as http]))


(defn advertise
  "Issue a PUT request to the advertise endpoint (ADDRESS/advertise)
  with MESSAGE as the body of the post and return the response.  The
  MESSAGE is expected to be a map of node-identifiers (see config).

  Returns a merged node map or nil if endpoint could not be
  contacted.
  "
  [address message]
  (try
    (let [url (format "http://%s/advertise" address)]
      (:body (http/put url {:content-type "application/edn"
                     :accept "application/edn"
                     :body (pr-str message)
                     :as :clojure})))
    (catch java.net.ConnectException e
      (log/error "advertise: client-put exception %s" (.getMessage e))
      nil)))


(defn campaign
  "Issue a PUT request to the campaign endpoint (ADDRESS/campaign) with NODE-INFO
as the body of the post and return the response.

Returns a map with the following information (at a minimum):
  {:status <http-status-code> :content-type <content-type> :body <body-of-response>
See: cluster/campaign-handler for more details
  "
  [node-info]
  (try
    (let [url (format "http://%s/campaign" (config/node-info-address node-info))]
      (log/debugf "campaign: node-info=%s, url=%s" node-info url)
      (http/put url {:content-type "application/edn"
                     :accept "application/edn"
                     :body (pr-str node-info)
                     :throw-exceptions false  ; let us handle 409's
                     :as :clojure}))
    (catch java.net.ConnectException e
      (log/error "campaign: client-put exception %s" (.getMessage e))
      {:status 503 :content-type "application/edn" :body node-info})))
