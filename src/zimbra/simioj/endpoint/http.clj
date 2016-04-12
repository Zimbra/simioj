(ns zimbra.simioj.endpoint.http
  (:require
   [clojure.core.async :refer [chan <!! >!! close!]]
   [clojure.tools.logging :as logger]
   [ring.adapter.jetty :refer [run-jetty]])
  (:gen-class))


;; (defn start! [ctx]
;;   "Given the context CTX, which is wrapped in a ref, start the http endpoint.
;; Will update CTX by adding an :endpoint instance."
;;   (let [server (get-in [:endpoint :server] @ctx)]
;;     (if server
;;       (.start server)
;;       (let [http-endpoint (get-in [:config :node :endpoint :http] @ctx)
;;             host-port (.split http-endpoint ":")
;;             host (first host-port)
;;             port (Integer/parseInt (second host-port))]
;;         (run-jetty (endpoint-app ctx) {:host host :port port :join? false})))))

;; (defn stop
;;   "Stop the node."
;;   []
;;   (when (-server)
;;     (.stop (-server)))
;;   @endpoint-state)
