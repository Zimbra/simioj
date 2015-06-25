(ns zimbra.simioj.raft.statemachine
  (:require [clj-http.client :as http]
            [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.tools.logging :as logger]
            [clojure.tools.reader.edn :as edn]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.raft.log :refer :all] ; [first-id-term last-id-term Log]]
            [zimbra.simioj.util :as util])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Protocol
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol StateMachine
  "The StateMachine receives commands from (internal) clients.  Each command
  is persisted into the leader's log first and then replicated in parallel
  to the followers.  Once a quorum of servers have persisted the command
  their log, it is said to be committed.  Committed commands are applied
  to the state.  The Raft protocol guarantees that all commands received
  by the system are applied to the state in the same order on all servers."
  (process-log! [this log servers-config server-state]
    "Using data from SERVER-STATE and (possibly) SERVERS-CONFIG,
     process applicable LOG entries and
     update the last-applied map in the SERVER-STATE.
     NOTE: SERVERS-CONFIG and SERVER-STATE are refs
    "
    )
  (get-state [this] [this resource-id] [this resource-id default]
    "Retrieve the computed state of the StateMachine If RESOURCE-ID is supplied
    (and if it is supported by the StateMachine, return the computed state
    of the specified resource.  If a resource with RESOURCE-ID does not
    exist and DEFAULT is supplied, that is returned, else nil is returned.

    Response: The requested state value
    ")
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Memory State Machine Implementation (testing only)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti msm-process-log-command!
  "Used by MemoryStateMachine instance to process log commands
   Parameters:
     idx - the log index
     cmd-key - the command to be processed; e.g., :noop, :set-config, :patch
     cmd-val - the value associated with the commadn
     servers-config (ref) - the Raft servers-config
     server-state (ref) - the Raft server-state
     cache - Any state-machine-specific cache
   Returns: true if command could be processed, else false
  "
  (fn [idx cmd-key cmd-val servers-config server-state cache] cmd-key)
  :default :noop)

(defmethod msm-process-log-command! :patch
  [idx cmd-key cmd-val servers-config server-state cache]
  (let [commit-index (@server-state :commit-index)]
    (if (<= idx commit-index)
      (let [{:keys [:oid :ops :upsert]} cmd-val
        res-old (@cache oid)
        res-new (or (reduce (fn [r f] (f r)) (or res-old upsert) ops) res-old)]
        (when (not= res-old res-new)
          (dosync (alter cache assoc oid res-new)))
        true)
      false)))

(defmethod msm-process-log-command! :set-config
  ;; config changes are always applied, whether or not
  ;; the command has been committed
  [idx cmd-key cmd-val servers-config server-state cache]
  (dosync (alter servers-config merge cmd-val))
  true)


(defmethod msm-process-log-command! :noop
  [idx cmd-key cmd-val servers-config server-state cache]
  (let [commit-index (@server-state :commit-index)]
    (if (<= idx commit-index)
      true
      false)))


(deftype MemoryStateMachine [id cache]
  StateMachine
  (process-log! [this log servers-config server-state]
    (logger/debugf "process-log!: id=%s, servers-config=%s, server-state=%s"
                id
                @servers-config
                @server-state)
    (let [last-applied-map ((@server-state :last-applied {}) id {:noop 0 :set-config 0 :patch 0})
          min-last-applied (apply min (vals last-applied-map))
          [lidx lterm] (last-id-term log)
          examine-indices (range (inc min-last-applied) (inc lidx))]
      (logger/debugf "process-log!: id=%s, last-applied-map=%s"
                     id
                     last-applied-map)
      (loop [indices examine-indices
             last-applied-map last-applied-map]
        (if (empty? indices)
          (dosync (alter server-state assoc-in [:last-applied id] last-applied-map))
          (let [i (first indices)
                entry (get-entry log i)]
            (when entry  ; safety check
              (let [[cmd-key cmd-val] (:command entry)
                    was-applied (and (< (last-applied-map cmd-key 0) i)
                                     (msm-process-log-command! i cmd-key cmd-val
                                                               servers-config server-state
                                                               cache))]
                (if was-applied
                  (recur (rest indices) (assoc last-applied-map cmd-key i))
                  (recur (rest indices) last-applied-map)))))))))
  (get-state [this]
    @cache)
  (get-state [this resource-id]
    (get-state this resource-id nil))
  (get-state [this resource-id default]
    (@cache resource-id default)))


(defn make-memory-state-machine
  ([id] (make-memory-state-machine id {}))
  ([id cache] (->MemoryStateMachine id (ref cache))))
