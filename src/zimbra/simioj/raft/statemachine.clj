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
  by the system are applied to the state in the same order on all servers.

  There is one exception to the \"committed\" rule.  The state machine that
  is responsible for processing Raft server configuration changes
  ALWAYS processes a :set-config log entry immediately, regardless of the
  commit-index.

  Implementer's Notes:

  1. The Raft :leader has to invoke the state machine's process-log!
     twice for every command that it receives.  It is invoked immediately
     after storing the command locally.  This gives any state machines
     that can operate on pre-committed log entries the opportunity
     to do so.  The state machine that processes :set-config commands
     it the primary one that needs to run.  Then, after it has replicated
     the command to its followers, the process-log! must be invoked again
     to allow log entries that are now commited to be processed.

     Therefore, when implementing a state machine, the first thing it
     should do is check to see if it is allowed to run by comparing
     its last-applied with the commit-index.
  2. If a state machine encounters a commited log command that it
     does not handle, it should still increment it's last-applied
     value.
  "
  (process-log! [this log commit-index last-applied]
    "Process applicable log entries.
     Parameters:
       LOG - A Raft log instance
       COMMIT-INDEX - The Server's commit index
       LAST-APPLIED - The last log entry applied by this state machine
     Returns an updated value to use for :last-applied.  The server is
     responsible for remembering this.
    "
    )
  (get-state [this] [this resource-id] [this resource-id default]
    "Retrieve the computed state of the StateMachine If RESOURCE-ID is supplied
    (and if it is supported by the StateMachine, return the computed state
    of the specified resource.  If a resource with RESOURCE-ID does not
    exist and DEFAULT is supplied, that is returned, else nil is returned.

    Response: The requested state value
    ")
  (add-state-change-listener [this listener]
    "State machines may choose to, by design, provide notifications when
     their state changes.  Interested parties may register a function that
     will by called with the following arguments when the state changes:
         (<old-state> <new-state>)
     What is meant by \"state\" may vary depending upon the state machine
     implementation.   For example, the state machine that handles
     object patches may choose to return the original state of the object
     (before appling a patch) as old-state and the new state of the object
     (after applying the patch) as the new-state.
     Returns the current state as of the time this function is called.
     ")
  (remove-state-change-listener [this listener]
    "Deregister the specified LISTENER function.  Returns a non-nil value
    if LISTENER was found, else nil.")
  (notify-state-changed [this old-state new-state]
    "Notify all of the registered listeners of a state change."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Sample memory State Machine Implementation (testing only)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti msm-process-log-command!
  "Used by MemoryStateMachine instance to process log commands
   Parameters:
     my-cmd - the command my instance is interested ing
     idx - the log index
     cmd-key - the command to be processed; e.g., :noop, :set-config, :patch
     cmd-val - the value associated with the command
     commit-index - the server's commit index
     cache - Any state-machine-specific cache (ref)
   Returns: {:applied? <bool> :state-changed? <bool> :old-state <map> :new-state <map>}
  "
  (fn [my-cmd idx cmd-key cmd-val commit-index cache] my-cmd)
  :default :noop)


(defmethod msm-process-log-command! :patch
  [my-cmd idx cmd-key cmd-val commit-index cache]
  (if (<= idx commit-index)
    (if (= my-cmd cmd-key)
      (let [{:keys [:oid :ops :upsert]} cmd-val
            res-old (@cache oid)
            res-new (or (reduce (fn [r f] (f r)) (or res-old upsert) ops) res-old)]
        (if (not= res-old res-new)
          (do
            (dosync (alter cache assoc oid res-new))
            {:applied? true
             :state-changed? true
             :old-state {:oid oid :val res-old}
             :new-state {:oid oid :val res-new}})
          {:applied? true :state-changed? false}))
      {:applied? true :state-changed? false})
    {:applied? false :state-changed? false}))

(defmethod msm-process-log-command! :set-config
  ;; config changes are always applied, whether or not
  ;; the command has been committed
  [my-cmd idx cmd-key cmd-val commit-index cache]
  (if (= my-cmd cmd-key)
    (let [old-state @cache
          new-state (dosync (alter cache merge cmd-val))]
      {:applied? true
       :state-changed? (not= old-state new-state)
       :old-state old-state
       :new-state new-state})
    {:applied? true :state-changed? false}))


(defmethod msm-process-log-command! :noop
  [my-cmd idx cmd-key cmd-val commit-index cache]
  (if (<= idx commit-index)
    {:applied? true :state-changed? false}
    {:applied? false :state-changed? false}))


(deftype MemoryStateMachine [cmd cache listeners]
  StateMachine
  (process-log! [this log commit-index last-applied]
    (logger/debugf "process-log!: cmd=%s, commit-index=%s, last=applied=%s"
                cmd commit-index last-applied)
    (let [[lidx lterm] (last-id-term log)
          examine-indices (range (inc last-applied) (inc lidx))]
      (loop [indices examine-indices
             new-last-applied last-applied]
        (if (empty? indices)
          new-last-applied
          (let [i (first indices)
                entry (get-entry log i)]
            (when entry  ; safety check
              (let [[cmd-key cmd-val] (:command entry)
                    {:keys [:applied? :state-changed? :old-state :new-state]}
                    (msm-process-log-command! cmd i cmd-key cmd-val commit-index cache)]
                (when state-changed?
                  (notify-state-changed this old-state new-state))
                (if applied?
                  (recur (rest indices) i)
                  (recur '() new-last-applied)))))))))
  (get-state [this]
    @cache)
  (get-state [this resource-id]
    (get-state this resource-id nil))
  (get-state [this resource-id default]
    (@cache resource-id default))
  (add-state-change-listener [this listener]
    (dosync (alter listeners conj listener))
    (get-state this))
  (remove-state-change-listener [this listener]
    (dosync (ref-set listeners (remove #(= listener %) @listeners))))
  (notify-state-changed [this old-state new-state]
    (dorun (pmap #(apply % [old-state new-state]) @listeners))))



(defn make-memory-state-machine
  "Construct an instance of a MemoryStateMachine.
   Parameters;
     cmd - The log command this state machine should handle.  Actual production
       state machines wouldn't necessarily need this parameter.  It is required
       here because the sample MemoryStateMachine uses a multi-method to dispatch
       on the various commands that it can process and we need a single
       state machine instance for each command.
     cache - A map, wrapped in a ref.
       If it contains data, that will be the initial state of the
       state machine.  This function will wrap that in a ref.
     listeners - An optional list of listener functions that will be called
       when this machine's state changes.
  "
  [cmd cache & listeners] (->MemoryStateMachine cmd cache (ref listeners)))
