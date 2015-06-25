(ns zimbra.simioj.discovery
  (:require [clojure.string :as string]
            [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.tools.logging :as logger]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.config :as config]
            [zimbra.simioj.messaging :as msg]
            [zimbra.simioj.util :refer [make-persisting-ref]])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Protocol Definition
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol Discovery
  (discover [this]
    "Perform discovery.  Returns new topology map.")
  (merge! [this new-topology]
    "Merges the NEW-TOPOLOGY map with our known-topology and returns
     the merged topology.  TOPOLOGY has this form:
    {<node-id> {:name <node-name> :address <address:port>}
     ...
    }")
  (add-topology-change-listener [this listener]
    "Register a function to be called when the cluster
     topology channges.  The function should have the
     following signature:
       (fn [old-topology new-topology])
     Where OLD-TOPOLOGY may be nil.")
  (notify [this old new]
    "Notify all registered topology-change listeners")
  (get-topology [this]
    "Return current known topology"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Protocol Implementation
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- ^:no-doc timestamp-now
  "Returns number of seconds since the epoch"
  []
  (quot (.getTime (java.util.Date.)) 1000))

(defn- ^:no-doc have-cluster?
  "Given the local configuration (CFG), this predicate will return true if the following
  conditions are met:
  1. The number of nodes we \"know\" about is >= the :expected-nodes
  2. Or all of following apply:
     a. We have a quorum, and
     b. The amount of time that has passed since we reached a quorum +
        the :quorum-timeout >= now.
  "
  [{:keys [:cfg :topology :quorum-time]}]
  (let [min-quorum (config/cluster-quorum cfg)
        exp-nodes (:expected-nodes (:discovery cfg))
        qtime @quorum-time
        qtimeout (:quorum-timeout (:discovery cfg))
        num-nodes (count @topology)]
    (when (and (>= num-nodes min-quorum)
               (zero? qtime))
      (dosync (ref-set quorum-time (timestamp-now))))

    (logger/debugf "have-cluster? id=%s, min-quorum=%s, exp-nodes=%s, qtime=%s, qtimeout=%s, num-nodes=%s\ntopology=%s"
                (.getId (Thread/currentThread))
                min-quorum exp-nodes qtime qtimeout num-nodes topology)
    (or (>= num-nodes exp-nodes)
        (and (>= num-nodes min-quorum)
             (> (+ qtime qtimeout) (timestamp-now))))))


(defmulti cd-discover
  "Multimethod to define protocol for cluster discovery.
  Takes a ClusterDiscovery instance as the only argument."
  (fn [{:keys [:cfg] :as cd}]
    (:method (:discovery cfg))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Unicast Discovery
;;;
;;; 1. We start with a seed node
;;; 2. Each node in the cluster advertises its presence to the seed node
;;; 3. The seed node replies with all the nodes it knows about and also
;;;    updates any other nodes any time a new node is added.
;;; 4. We are done one we have the expected number of nodes, or
;;;    we have a quorum of the expected number of nodes and we pass
;;;    a configurable timeout.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod cd-discover :unicast [{:keys [:cfg :topology] :as cd}]
  ;; TODO - update to use all of the servers listed in
  ;; initial-nodes
  (let [listener-addr (first (:initial-nodes (:discovery cfg)))]
    (logger/debugf "discover %s: listener-addr='%s'"
               (.getId (Thread/currentThread))
               listener-addr)
    (loop [nodes (merge @topology (config/local-node-info cfg))]
      (Thread/sleep 1000)
      (if (have-cluster? cd)
        (do
          (notify cd @topology nodes)
          (dosync (ref-set topology nodes))
          nodes)
        (recur (merge @topology (msg/advertise listener-addr nodes)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; TODO - Multicast Discovery
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defmethod cd-discover :multicast [_]
;;   ;; TODO
;;   )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Disabled Discovery (single node cluster)
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmethod cd-discover :disabled [{:keys [:cfg :topology] :as cd}]
  (dosync (ref-set topology (config/local-node-info cfg)))
  (notify cd {} @topology)
  @topology)


(defn- cd-add-topology-change-listener [{:keys [:listeners]} listener]
  (dosync (alter listeners conj listener)))


(defn- cd-merge! [{:keys [:topology]} new-topology]
  (dosync (ref-set topology (merge @topology new-topology)))
  @topology)

(defn- cd-notify [{:keys [:listeners]} old new]
  (dorun (pmap #(% old new) @listeners)))

(defn- cd-get-topology [{:keys [:topology]}]
  topology)


(defrecord ClusterDiscovery [cfg           ; map of :local config and :cluster state
                             topology      ; [ref] working cluster topology map
                             quorum-time   ; [ref] used to track quorum time (ms)
                             listeners])   ; [ref] list of listeners


(extend ClusterDiscovery
  Discovery
  {:add-topology-change-listener cd-add-topology-change-listener
   :discover cd-discover
   :merge! cd-merge!
   :notify cd-notify
   :get-topology cd-get-topology})

(actor-wrapper ClusterDiscoveryActor [Discovery] defrecord)

(defn make-cluster-discovery
  "Constructs/returns a ClusterDiscovery instance, wrapped into an actor,
  and started.
  Parameters:
    CFG - A map containing {:local <local-config> :cluster <cluster-state>}
    LISTENERS - Optional topology-change-listener functions that will be
      called anytime the topology map of known nodes is updated.  It is
      is called with 2 arguments: <old-topology> <new-topology>."
  [cfg & listeners]
  (let [t (make-persisting-ref
           (.getAbsolutePath (clojure.java.io/file
                              (:data-dir (:node cfg))
                              "topology.edn")) :default {})
        sd (->ClusterDiscovery  cfg t (ref 0) (ref listeners))
        sda (->ClusterDiscoveryActor (chan) sd)]
    (start sda)
    sda))

(defn stop-discovery [d]
  (stop d))
