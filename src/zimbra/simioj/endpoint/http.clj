(ns zimbra.simioj.endpoint.http
  (:require
   [clj-http.client :as http]
   [clojure.core.async :refer [chan <!! >!! close!]]
   [clojure.tools.logging :as logger]
   [compojure.core :refer :all]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.util.request :refer :all]
   [zimbra.simioj.endpoint.middleware :refer :all]
   [zimbra.simioj.raft.server :refer :all])
  (:gen-class))


(defn- http-status
  "Convert a status keyword as returned by comman! to
  the appropriate HTTP status code"
  [{:keys [:status] :or {:status :accepted}}]
  (condp = status
    :ok 200
    :accepted 202
    :conflict 409
    :moved 301
    :found 302
    :not-found 404
    :unavailable 503
    503))



(defn append-entries-handler
  [ctx req]
  (let [entries (read-string (slurp (:body req)))
        raft (:raft @ctx)
        resp (append-entries raft (->Entry
                                   (:term entries)
                                   (:leader-id entries)
                                   (:prev-log-index entries)
                                   (:prev-log-term entries)
                                   (:entries entries)
                                   (:leader-commit entries)))]
    {:status 202
     :body resp}))

(defn info-handler
  "Used to communicate knowledge of nodes around the cluster."
  [ctx req]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body "Supported methods:

GET /status - returns the status of this Raft Server
GET /state/<state-machine> - returns the current value of the
    specified state machine
GET /state/<state-machine>/<resource-id> - returns the
    current value of the specified <resource-id> from
    the specified <state-machine>
PUT /leader/<new-leader-id> - updates the :leader attribute
    to <new-leader-id> which will set the leader affinity
    to the specified raft
POST <command> - Post a command to the Raft
    X-Request-ID (header) - If specified, uses the provided request
        ID instead of generating one."})

(defn post-handler
  "Post a command to the Raft"
  [ctx req]
  (let [headers (:headers req)
        rid (headers "x-request-id" (generate-rid))
        command (read-string (slurp (:body req)))
        raft (:raft @ctx)
        resp (command! raft rid command)
        status (http-status resp)
        ret {:status status :body resp}
        rpc (:rpc raft)]
    (if (contains? #{301 302} status)
      (assoc ret :headers {"Location" (format "http://%s/" (@(:servers rpc) (:server resp)))})
      ret)))


(defn request-vote-handler
  [ctx req]
  (let [vote (read-string (slurp (:body req)))
        raft (:raft @ctx)
        resp (request-vote raft (->Vote
                                 (:term vote)
                                 (:candidate-id vote)
                                 (:last-log-index vote)
                                 (:last-log-term vote)))]
    {:status 200
     :body resp}))

(defn state-handler
  "Retrieve state values"
  [ctx state-machine resource-id req]
  (logger/infof "state-handler: state-machine=%s (%s), resource-id=%s (%s)"
                state-machine (type state-machine)
                resource-id (type resource-id))
  (let [raft (:raft @ctx)
        sm (keyword (clojure.string/replace state-machine #":" ""))
        resp (if (nil? resource-id)
               (get-machine-state raft sm)
               (get-machine-state raft sm resource-id))
        status (http-status resp)
        ret {:status status :body (:state resp)}
        rpc (:rpc raft)]
    (if (contains? #{301 302} status)
      (assoc ret :headers {"Location" (format "http://%s/state/%s/%s" (@(:servers rpc) (:server resp)) state-machine resource-id)})
      ret)))


(defn status-handler
  "Return the status of this Raft server"
  [ctx req]
  (let [raft (:raft @ctx)]
    (logger/tracef "status-handler: id=%s, servers-config=%s"
                   (:id raft)
                   (:servers-config raft))
    {:status 200
     :body {:id (:id raft)
            :servers-config (assoc @(:servers-config raft) :state-machines (keys (:state-machines @(:servers-config raft))))
            :server-state @(:server-state raft)
            :leader-state @(:leader-state raft)}}))

(defn set-leader-handler
  "Update the :leader attribute of the :servers-config to the specified
  value"
  [ctx id req]
  (let [raft (:raft @ctx)
        new-leader (keyword (clojure.string/replace id #":" ""))]
    (dosync
     (alter (:servers-config raft) assoc :leader new-leader)))
  (status-handler ctx req))


(defn app-routes [ctx]
  (routes
   (GET "/" [] (partial info-handler ctx))
   (GET "/status" [] (partial status-handler ctx))
   (context "/state/:state-machine" [state-machine]
            (GET "/" [] (partial state-handler ctx state-machine nil))
            (GET "/:resource-id" [resource-id] (partial state-handler ctx state-machine resource-id)))
   (POST "/" [] (partial post-handler ctx))
   (PUT "/leader/:id" [id] (partial set-leader-handler ctx id))
   (context "/raft" []
            (POST "/append-entries" [] (partial append-entries-handler ctx))
            (POST "/request-vote" [] (partial request-vote-handler ctx)))))


(defn- endpoint-app
  "Wraps the provided routes with json or edn filtering"
  [ctx]
  (wrap-response
   (routes
    (app-routes ctx))))


(defn start!
  "Given the context CTX, which is wrapped in a ref, start the http endpoint.
  Will update CTX by adding an :endpoint instance."
  [ctx]
  (let [server (get-in [:endpoint :server] @ctx)]
    (if server
      (.start server)
      (let [http-endpoint (get-in @ctx [:config :node :endpoint :http])
            host-port (.split http-endpoint ":")
            host (first host-port)
            port (Integer/parseInt (second host-port))]
        (run-jetty (endpoint-app ctx) {:host host :port port :join? false})))))

;; (defn stop
;;   "Stop the node."
;;   []
;;   (when (-server)
;;     (.stop (-server)))
;;   @endpoint-state)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; HTTP RPC
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn post-edn
  [address path message]
  (try
    (let [url (format "http://%s/%s" address path)]
      (http/post url {:content-type "application/edn"
                             :accept "application/edn"
                             :body (pr-str message)
                             :as :clojure}))
    (catch java.net.ConnectException e
      (logger/error "post-message: exception %s" (.getMessage e))
      {:status 503 :content-type "application/edn" :body message})))


;;; servers - map (wrapped in a ref) of the following form:
;;;          {<id0> <ip:port>, ...}
(defrecord HttpRpc [servers]
  RaftProtocol
  ;; TODO - filter nil responses
  (append-entries [this server-ids entries]
    (let [body (into {} entries)
          path "raft/append-entries"]
      (logger/tracef "httprpc/append-entries: server-ids=%s, body=%s" server-ids body)
      (apply merge (pmap
                    (fn [s]
                      (let [resp (post-edn (@servers s) path body)]
                        (logger/tracef "httprpc/append-entries: resp=%s (%s)" resp (type resp))
                        {s (condp = (:status resp)
                             202 (:body resp)
                             nil)}))
                    server-ids))))

  (request-vote [this server-ids vote]
    ;; TODO - filter nil responses
    (let [body (into {} vote)
          path "raft/request-vote"
          resp (apply merge (pmap
                             (fn [s]
                               (let [resp (post-edn (@servers s) path body)]
                                 (logger/tracef "httprpc/request-vote: resp=%s (%s)" resp (type resp))
                                 {s (condp = (:status resp)
                                      200 (:body resp)
                                      nil)}))
                             server-ids))]
      (logger/tracef "httprpc/request-vote: server-ids=%s, vote=%s, @servers=%s, resp=%s"
                     server-ids vote @servers resp)
      resp))



  (install-snapshot [this server-ids snapshot] nil)
    ;; (apply merge (pmap (fn [s] {s (install-snapshot (@server-map s) snapshot)}) server-ids))))
  )


(defn make-http-rpc
  "Contructs an HTTP-RPC instance.  Accepts a map of the following form:
  {:servers {<id> {:endpoint {:http \"ip:port\"}}
             ...}}
  "
  [{:keys [:servers]}]
  (let [server-map (into {} (map (fn [[i e]] [i (:http (:endpoint e))]) servers))]
    (logger/tracef "make-http-rpc: servers=%s, server-map=%s" servers server-map)
    (->HttpRpc (ref server-map))))
