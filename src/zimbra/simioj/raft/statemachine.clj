(ns zimbra.simioj.raft.statemachine
  (:require [clj-http.client :as http]
            [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.tools.logging :as logger]
            [clojure.tools.reader.edn :as edn]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.raft.log :refer :all]
            [zimbra.simioj.util :as util])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Protocols
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

  Our implementation of StateMachine allows for the registration of
  an arbitrary number of StateProcessors.  Each StateProcessor
  can operate on a given command. More than one StateProcessor
  can operate on the same command.

  A StateProcessor implements the following functions from the StateMachine
  protocol:

    process-command!
    get-state

  Implementation Notes

  Any processor implementation must provide a function to construct an instance
  of that processor.  It is required that all such functions accept an first
  DIRS-MAP artument that is a map with the following entries.
    {:config <config-dir-path>
     :state <state-dir-path>
     :snapshots <snapshots-dir-path>}
  These directories are guaranteed to pre-exist.  It is not required that
  the specific implmentation use this argument.
  "
  (process-set-config! [this command]
    "Immediately process COMMAND, if it is a :set-config. Else
    is a no-op.
    Returns:
    {:applied? true|false
     :state-changed? true (if :applied? and :old-state != :new-state)
     :old-state <old-state> (if :applied?, else not included)
     :new-state <new-state> (if :applied?, else not included)}
    ")
  (process-command! [this command]
    "Process COMMAND from log.  The command must have been committed before
     calling this function.
     Parameters:
       LOG - A Raft log instance
       COMMAND - The command to process.
    Returns:
    {:applied? true|false
     :state-changed? true (if :applied? and :old-state != :new-state)
     :old-state <old-state> (if :applied? and :state-changed?, else not included)
     :new-state <new-state> (if :applied? and :state-changed?, else not included)}
    ")
  (get-state [this] [this resource-id] [this resource-id default]
    "Retrieve the computed state of the StateMachine If RESOURCE-ID is supplied
    (and if it is supported by the StateMachine, return the computed state
    of the specified resource.  If a resource with RESOURCE-ID does not
    exist and DEFAULT is supplied, that is returned, else nil is returned.

    Response: The requested state value
    "))


(defprotocol StateChangedNotifier
    "State machines may choose to, by design, provide notifications when
     their state changes.  Interested parties may register a function that
     will by called with the following arguments when the state changes:
         (<command-id> <old-state> <new-state>)
     What is meant by \"state\" may vary depending upon the state machine
     implementation.   For example, the state machine that handles
     object patches may choose to return the original state of the object
     (before appling a patch) as old-state and the new state of the object
     (after applying the patch) as the new-state."

  (notify-state-changed [this command-id old-state new-state]
    "Notify all of the registered listeners of a state change.
     Parameters:
     COMMAND-ID - The command identifier; e.g., :set-config, :patch, etc.
     OLD-STATE - The state prior to command application
     NEW-STATE - The state after command application"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Sample memory State Processor Implementation (testing only)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti msm-process-command!
  "Used by MemoryStateProcessor instance to process log commands
   Parameters:
     my-cmd - the command my instance is interested in
     cmd-key - the command to be processed; e.g., :noop, :set-config, :patch
     cmd-val - the value associated with the command
     cache - Any state-machine-specific cache (ref)
   Returns: {:applied? <bool> :state-changed? <bool> :old-state <map> :new-state <map>}
  "
  (fn [my-cmd cmd-key cmd-val cache] my-cmd)
  :default :noop)


(defmethod msm-process-command! :patch
  [my-cmd cmd-key cmd-val cache]
  (if (= my-cmd cmd-key)
    (let [{:keys [:oid :ops :upsert]} cmd-val
          res-old (@cache oid)
          res-new (or (reduce (fn [r f]
                                ((if (fn? f) f (eval f)) r)) (or res-old upsert) ops) res-old)]
      (if (not= res-old res-new)
        (do
          (dosync (alter cache assoc oid res-new))
          {:applied? true
           :state-changed? true
           :old-state {:oid oid :val res-old}
           :new-state {:oid oid :val res-new}})
        {:applied? true :state-changed? false}))
    {:applied? true :state-changed? false}))


  (defmethod msm-process-command! :noop
    [my-cmd cmd-key cmd-val cache]
    {:applied? true :state-changed? false})



(deftype MemoryStateProcessor [cmd cache]
  StateMachine
  (process-command! [this [cmd-key cmd-val]]
    (msm-process-command! cmd cmd-key cmd-val cache))
  (get-state [this]
    @cache)
  (get-state [this resource-id]
    (get-state this resource-id nil))
  (get-state [this resource-id default]
    (@cache resource-id default)))



(defn make-memory-state-processor
  "Construct an instance of a MemoryStateProcessor.
   Parameters: A map with the following keys, at a minimum:
     _dirs_map - not used
     cmd - The log command this state machine should handle.  Actual production
       state machines wouldn't necessarily need this parameter.  It is required
       here because the sample MemoryStateMachine uses a multi-method to dispatch
       on the various commands that it can process and we need a single
       state machine instance for each command.
     cache - A map, wrapped in a ref.
       If it contains data, that will be the initial state of the
       state machine.  This function will wrap that in a ref.
  "
  [_dirs-map cmd cache]
  (->MemoryStateProcessor cmd cache))
