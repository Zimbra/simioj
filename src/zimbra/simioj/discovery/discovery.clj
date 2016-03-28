(ns zimbra.simioj.discovery.discovery
  (:require [clojure.string :as string]
            [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.tools.logging :as logger]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.config :as config]
            [zimbra.simioj.discovery.messaging :as msg]
            [zimbra.simioj.util :refer [make-persisting-ref]])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Utility Functions
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn cluster-quorum
  "Given the EXPECTED-NODES for our cluster,
   return the number of nodes required to make quorum."
  [expected-nodes]
  (inc (bit-shift-right expected-nodes 1)))

(defn timestamp-now
  "Returns number of milliseconds since the epoch"
  []
  (.getTime (java.util.Date.)))

(defn- ^:no-doc recently-contacted-nodes
  "Given a NODES map, returns a map that filters out
  all nodes that have not been contacted within eviction-time
  milliseconds"
  [nodes eviction-time]
  (let [now (timestamp-now)]
    (into {} (remove (fn [[id v]]
                       (< (+ (v :contacted now) eviction-time) now))
                     nodes))))


(defn update-toplogy-clustered
  "Given a TOPOLOGY map, will check to see if we know about enough
  nodes to establish a cluster.
  Parameters:
    TOPOLOGY - a topology map
    EXPECTED-NODES - The number of nodes that (by our configuration)
      that we expect to be part of the cluster
    QUORUM-TIMEOUT - The amount of time (in ms) that must pass for
      a quorum of nodes to be considered valid to establish a cluster
    EVICTION-TIME - The amount of time (in ms) after which we
      consider too much time to have passed for a node to be considered
      to be part of the cluster.
  Returns:
    The TOPOLOGY map with :quorum-time and :have-cluster? possibly
    updated
  Algorithm:
    1. The number of valid nodes is >= the :expected-nodes
    2. Or all of following apply:
       a. We have a quorum, and
       b. The amount of time that has passed since we reached
          a quorum (:quorum-time topology) + QUORUM-TIMEOUT
          < <now>.
  "
  [topology expected-nodes quorum-timeout eviction-time]
  (let [min-quorum (cluster-quorum expected-nodes)
        recent-nodes (recently-contacted-nodes (:nodes topology) eviction-time)
        num-nodes (count recent-nodes)
        now (timestamp-now)
        quorum-time (if (and (>= num-nodes min-quorum)
                             (zero? (topology :quorum-time 0)))
                      now
                      (topology :quorum-time 0))
        have-cluster? (or (>= num-nodes expected-nodes)
                          (and (>= num-nodes min-quorum)
                               (< (+ quorum-time quorum-timeout) now)))]
    (assoc topology :quorum-time quorum-time :have-cluster? have-cluster?)))

(defn- ^:no-doc gen-node-addresses
  "Generate a seq of node addresses that may be used by a
  DiscoveryRPC instance for the purposes of adversising."
  [& {:keys [:seed-nodes :topology] :or
    {:seed-nodes [] :topology {:have-cluster? false :nodes {}}}}]
  (seq (set (concat seed-nodes
                    (map :address (vals (:nodes topology)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Protocol Definition
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol Discovery
  (discover [this]
    "Perform discovery.  Continues to loop until stopped.  Notifies
     all register topology-change-listeners as needed.")
  (notify! [this node-map]
    "Is called by other nodes when they want to notify us of their NODE-MAP.
     Our behavior is to update our topology accordingly and return a
     new NODE-MAP to the caller that is the ")
  (merge! [this new-topology]
    "Merges the NEW-TOPOLOGY map with our known-topology and returns
     the merged topology.  TOPOLOGY will contain the following minimum
     information:
    {:have-cluster? true (if minimum-expected-nodes) | false
     :nodes <node-map>}
    where <node-map> is:
      {<node-id> {:name <node-name> :address <address:port>}
       ...
      }")
  (add-topology-change-listener [this listener]
    "Register a function to be called when the cluster
     topology channges.  The function should have the
     following signature:
       (fn [old-topology new-topology])
     Where OLD-TOPOLOGY may be nil or empty map.
     Returns: the current topology.
     ")
  (notify [this old new]
    "Notify all registered topology-change listeners")
  (get-topology [this]
    "Return current known topology"))

(defprotocol DiscoveryRPC
  (notify-server! [this address node-map]
    "Present NODE-MAP to the the server at ADDRESS by invoking
    its notify! function.  This server is expected to update its own
    node-map with NODE-MAP and return the result.
    Implementation Node:  The RPC entity that implements this method
      should return nil if it cannot contact the server at ADDRESS."))


(defmulti cd-discover
  "Multimethod to define protocol for cluster discovery.
  Takes a ClusterDiscovery instance as the only argument."
  (fn [{:keys [:cfg] :as cd}]
    (:method cfg)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Unicast Discovery
;;;
;;; 1. We start with a seeded list of addresses (nodes)
;;; 2. Each node in the cluster advertises its presence
;;; 3. The seed node replies with all the nodes it knows about, merged with
;;;    any tolds it was told about.
;;; 4. notify when the following conditions occur:
;;;    - have-cluster? changes
;;;    - number of available nodes changes
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- cd-discover-advertise [rpc addresses node-map]
  (let [rmap (into {} (pmap (fn [a] {a (notify-server! rpc a node-map)}) addresses))
        gaddr (map first (filter second rmap))
        nodes (apply merge (map second (filter second rmap)))
        now (timestamp-now)]
    (into {} (map (fn [[id v]] {id (assoc v :contacted now)}) nodes))))


(defn- cd-discovery-loop [{:keys [:cfg :rpc :topology] :as cd}]
  (let [{:keys [:eviction-time :expected-nodes :initial-nodes :poll-interval :quorum-timeout]} cfg]
    (loop [addresses (gen-node-addresses :seed-nodes initial-nodes :topology @topology)]
      (let [old-avail-ids (keys (recently-contacted-nodes (:nodes @topology) eviction-time))
            nodes (cd-discover-advertise rpc addresses (:nodes @topology))
            new-topology (update-toplogy-clustered
                          (assoc @topology :nodes nodes)
                          expected-nodes
                          quorum-timeout eviction-time)]
        (logger/debugf "CD-DISCOVERY-LOOP: id=%s; OLD: cluster?=%s, nodes=%s; NEW: cluster?=%s, nodes=%s"
                       (:id @topology) (:have-cluster? @topology) old-avail-ids (:have-cluster? new-topology) (keys nodes))
        (when (or (not= (:have-cluster? @topology) (:have-cluster? new-topology))
                  (not= (keys nodes) old-avail-ids))
          (notify cd @topology new-topology)
          (dosync (ref-set topology new-topology))))
      (Thread/sleep poll-interval)
      (recur (gen-node-addresses :topology @topology)))))


;; Start the discovery loop.
(defmethod cd-discover :unicast [this]
  (logger/debug "CD-DISCOVER: starting discovery loop")
  (future (cd-discovery-loop this)))


(defn- cd-notify! [{:keys [:topology]} node-map]
  (let [merged-node-map (merge (:nodes @topology) node-map)]
    (dosync (alter topology assoc :nodes merged-node-map))
    merged-node-map))


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


(defrecord ClusterDiscovery [rpc           ; a DiscoveryRPC instance
                             cfg           ; :discovery config - see defaults.edn
                             topology      ; [ref] working cluster topology map
                             listeners])   ; [ref] list of listeners


(extend ClusterDiscovery
  Discovery
  {:add-topology-change-listener cd-add-topology-change-listener
   :discover cd-discover
   :merge! cd-merge!
   :notify cd-notify
   :notify! cd-notify!
   :get-topology cd-get-topology})

(actor-wrapper ClusterDiscoveryActor [Discovery] defrecord)


(defn- init-persistent-topology
  "Initialize the topology map that is used by Discovery.
  Returns a persisting-ref that wraps the topology map.
  Parameters:
    CFG - {:node <node-config> :discovery <discovery-config>} (see
      defaults.edn)
  Returns:
    {:id <local-node-id> :quorum-time 0 :have-cluster? false :nodes <node-map>}
  Where:
    <node-map> keys are node identifiers and values contain (at a
    minimum {:name <node-name> :address <node-address>}
  "
  [data-dir default]
  (let [local-topo-path (.getAbsolutePath (clojure.java.io/file
                                           data-dir
                                           "topology.edn"))]
    (make-persisting-ref local-topo-path :default default)))

(defn- init-topology [cfg]
  (let [local-node-info (config/local-node-info cfg)
        default {:id (first (keys local-node-info))
                 :have-cluster? false
                 :quorum-time 0
                 :nodes {}}
        data-dir (:data-dir (:node cfg))
        t-ref (if data-dir
                (init-persistent-topology data-dir default)
                (ref default))]
    (dosync
     (ref-set t-ref (-> @t-ref
                        (assoc :id (first (keys local-node-info)))
                        (assoc :have-cluster? false)
                        (assoc :quorum-time 0)
                        (assoc :nodes (merge (:nodes @t-ref) local-node-info)))))
    t-ref))


(defn make-cluster-discovery
  "Constructs/starts/returns a ClusterDiscovery instance, wrapped into
  an actor.
  Parameters:
    RPC - Something that implements DiscoveryRPC
    CFG - {:node <node-config> :discovery <discovery-config>} (see
      defaults.edn)
    LISTENERS - Optional topology-change-listener functions that will be
      called anytime the topology map of known nodes is updated.  It is
      is called with 2 arguments: <old-topology> <new-topology>."
  [rpc cfg & listeners]
  (let [topo-ref (init-topology cfg)
        sd (->ClusterDiscovery rpc (:discovery cfg) topo-ref (ref listeners))
        sda (->ClusterDiscoveryActor (chan) sd)]
    (start sda)
    sda))

(defn stop-discovery [d]
  (stop d))
