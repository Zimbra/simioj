(ns zimbra.simioj.raft.server
  (:require [clj-http.client :as http]
            [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.tools.logging :as logger]
            [clojure.tools.reader.edn :as edn]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.raft.log :refer :all]
            [zimbra.simioj.raft.statemachine :refer :all]
            [zimbra.simioj.util :as util])
  (:gen-class))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Raft
;;;;
;;;; Raft protocol definition
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord Entry [^Number term            ; leader's term
                  leader-id               ; leader's ID so followers can redirect
                  ^Number prev-log-index  ; idx of log entry immediatly preceeding
                  ^Number prev-log-term   ; term of prev-log-index entry
                  entries                 ; entries to store (empty for heartbeat)
                  ^Number leader-commit]) ; leader's commit-index

(defrecord Vote [^Number term             ; candidate's term
                 candidate-id             ; candidate requesting vote
                 ^Number last-log-index   ; index of candidate's last log entry
                 ^Number last-log-term])  ; term of candidate's last log entry

(defrecord Snapshot [^Number term         ; leader's term
                     leader-id            ; so followers can redirect clients
                     ;; the snapshot replaces all log entries up
                     ;; to and including this index
                     ^Number last-included-index
                     ;; term of last-included-index
                     ^Number last-included-term
                     ;; byte offset where chunk positioned in ss file
                     ^Number offset
                     ;; raw bytes of shapshot chunk, starting at offset
                     data
                     ^Boolean done])      ; true if this is the last chunk




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Servers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def server-states
  "Define the valid states of a Raft server"
  #{:follower      ; just following orders
    :candidate     ; seeking control
    :leader})      ; the boss


(defprotocol Election
  "Methods required to be implemented to perform Raft Election
  state transistions."
  (start-timers! [this]
    "Start all state-specific timers required for Election.")
  (cancel-timers! [this] [this timer]
    "Cancel all active Election timers or just the timer with key TIMER.")
  (follower! [this]
    "Transition the Raft server to :follower state.")
  (candidate! [this]
    "Possibly transition the Raft server to :candidate state.")
  (leader! [this] [this new-term]
    "The arity/1 function trigers the leader to broadcast an
     empty append-entries RPC to all of its follows if normal
     Raft election is being used.  If configured Raft election is
     being used this is a noop.
    The arity/2 function is called when a :candidate wins an
    election and needs to transition to :leader state."))




(defprotocol RaftProtocol
  "Methods required to be implemented for the Raft protocol.  Note that the
  following methods have two signatures:
    append-entries
    request-vote
    install-snapshot
    command!

  The form with the first signature is to be implemented by a Raft Server.
  The form with the second signature by any Raft RPC implementation.

  The command! method is used to submit a command to a Raft server.  This method
  does not currently require an RPC implementation.  If we decide to have
  Raft servers auto-forward commands to the leader (which we will probably do
  at some point), then we well need to add a second
  "
  (command! [this rid command] [this server-ids rid command]
    "Used by (internal) clients to submit a command to the system for
    application to the state.
    Parameters:
      RID - A client-generated request ID.  Every command submitted
        to the system must have a unique request ID.  It is recommended
        that the clients use either a UUID or a value of the form
        <client-id>-<request-serial-number> or some other construction
        that is guaranteed to be unique.  If a new command is submitted to
        the system that uses the same RID as a previous command, the
        system will not store the new command.
    Response: {:status :accepted   ; command accepted and committed
                       :conflict   ; duplicate RID
                       :moved      ; permanent redirect to :server
                       :found      ; temporary redirect to :server
                       :unavailable; unable to commit command (lack of quorum)
               :server <server-id> ; the server ID that request was sent to
                                   ; or the server ID that the request should
                                   ; be resent to in the case of a temporary
                                   ; or permanent redirect
              }
    Commands: One of the following (this may be refactored as a type later).
    It is always a vector of [<command-id> <command-data>].  There is always
    only ONE command per log entry.  If we encode more than one command per log entry
    there is a chance that one of the commands may not be able to be executed (due to the
    last-applied value, and this could lead to issues with certain commands being applied
    more than once.
       [:noop {}]  (used internally by server, here for documentation only)
       [:patch {:oid <objectid>
                :ops <list-of-patch-operations>
                :upsert <map-of-initial-state-for-new-object>}]
       [:event {:oid <objectid> (real or virtual)
                :eid <event-id>
                :op :push | :pop
                :payload {}}
       NOTE: Configuration updates are apply by server immediately upon
             receipt, before committing.  :new config is optional and is
             used only during state transitions.
       This command will update the :servers-config value.  Only keys that are present in the maps
       will be affected.  Currently that means:
         :servers - a vector of one or more sets of Raft server identifiers.
         :state-machines - a sequence of state machines instances
       [:set-config <new-configuration-map>]

    ")
  (get-machine-state [this machine] [this machine resource-id] [this machine resource-id default]
    "Retrieve the computed state of the StateMachine identified
    by MACHINE.  If RESOURCE-ID is supplied (and if it is applicable
    for MACHINE, return the computed state of the specified resource.
    If RESOURCE-ID does not exist return nil or DEFAULT if supplied.

     Response: {:status :ok          ; response in :object
                        :not-found   ; MACHINE
                        :moved       ; permanent redirect to :server
                        :found       ; temporary redirect to :server
                :server <server-id>  ; the server ID that request was sent to
                                     ; or the server ID that the request should
                                     ; be resent to in the case of a temporary
                                     ; or permanent redirect
                :state <state-map>   ; if :status = :ok
                       nil           ; otherwise
              }
    ")
  (append-entries [this entries] [this server-ids entries]
    "Invoked by leader to replicate log entries; also used as heartbeat.
    ENTRIES is an instance of Entry.
    SERVER-IDS is a list of one or more server IDs.
    The first form should return a map
      {:term <current-term>
       :success <boolean>
       ;; These used to speed repair.  If :success is true, the values
       ;; are meaningless and are not used.
       :conflicting-term <conflicting-term-number>
       :conflicting-first-idx <first-log-index-containing-conflicting-term>}
    The second form should return a map of
    {<server-id> {:term <current-term>
                  :success <boolean>
                  :conflicting-term <conflicting-term-number>
                  :conflicting-first-idx <first-log-index-containing-conflicting-term>}}
    ")
  (request-vote [this vote] [this server-ids vote]
    "Invoked by candidates to gather votes.
    VOTE is an instance of Vote.
    SERVER-IDS is a list of one or more server IDs.
    The first form should return a map {:term <current-term> :vote-granted <boolean>}
    The second form should return a map of
    {<server-id> {:term <current-term> :vote-granted <boolean>}}")
  (install-snapshot [this snapshot] [this server-ids snapshot]
    "Invoked by leader to send chunks of a snapshot to a follower.
    Leaders always send chunks in order.
    SNAPSHOT is an instance of Snapshot.
    SERVER-IDS  is a single-valued list containing theid of the server that the
    leader is replicating the snapshot to.
    The first form should return a map {:term <current-term>}.
    The second form should return a map {<server-id> {:term <current-term>}}"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Test Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;; The server-map here has the following structure:
;;;   key = <server-identifier>
;;;   value = <server-instance>
;;; NOTE: It should be wrapped in a ref!
;;; It is intended for use in testing
(deftype TestRpc [server-map]
  RaftProtocol
  (append-entries [this server-ids entries]
    (apply merge (pmap (fn [s] {s (append-entries (@server-map s) entries)}) server-ids)))
  (request-vote [this server-ids vote]
    (apply merge (pmap (fn [s] {s (request-vote (@server-map s) vote)}) server-ids)))
  (install-snapshot [this server-ids snapshot]
    (apply merge (pmap (fn [s] {s (install-snapshot (@server-map s) snapshot)}) server-ids))))

;;; log must be a vectory wrapped in a ref
;;; see make-memory-log
(deftype MemoryLog [log]
  Log
  (first-id-term [this]
    (let [entry (first @log)]
      (if entry
        [(:id entry) (:term entry)]
        [0 0])))
  (last-id-term [this]
    (let [ll (count @log)
          entry (when (pos? ll) (nth @log (dec ll)))]
      (if entry
        [(:id entry) (:term entry)]
       [0 0])))
  (post-cmd! [this term rid command]
    (let [[lid lterm] (last-id-term this)
          nid (inc lid)
          entry {:id nid :term term :rid rid :command command}
          nidx (count @log)]
      (when (not-empty (filter #(= (:rid %) rid) @log))
        (throw (IllegalArgumentException.
                (format "another entry with request id %s already in log" rid))))
      (dosync
       (alter log #(assoc % nidx entry)))
      nid))
  (put-cmd! [this id term rid command]
    (let [[lid lterm] (last-id-term this)
          entry {:id id :term term :rid rid :command command}
          nidx (count @log)]
      ;; (logger/debugf "put-cmd!: id=%s, term=%s, rid=%s, lid=%s, lterm=%d, nidx=%s"
      ;;             id term rid lid lterm nidx)
      (if (= lid (dec id))
        (dosync
         (alter log #(assoc % nidx entry))
         true)
        false)))
  (get-entry [this id]
    (let [[fid fterm] (first-id-term this)
          i (- id fid)
          clog (count @log)]
      ;; (logger/debugf "get-entry: id=%s, fid=%s, fterm=%s, i=%s, #log=%s"
      ;;             id fid fterm i clog)
      (when (and (not (neg? i)) (< i clog))
        (nth @log i))))
  (ltrim-log! [this last-id]
    (let [[fid fterm] (first-id-term this)
          ilast (- last-id fid)]
      (if (and (>= ilast 0)
               (< ilast (count @log)))
        (dosync
         (alter log #(subvec % (inc ilast)))
         true)
        false)))
  (rtrim-log! [this first-id]
    (let [[fid fterm] (first-id-term this)
          ilast+1 (- first-id fid)]
      (if (and (>= ilast+1 0)
               (< ilast+1 (count @log)))
        (dosync
         (alter log #(subvec % 0 ilast+1))
         true)
        false))))


(defn- persist-if-changed [location vref expr]
  (let [ref-val (deref vref)
        rvalue (expr)]
    (when-not (= ref-val (deref vref))
      (logger/debugf "Value was changed.. Persisting %s to %s" @vref location)
      (util/persist-obj location (deref vref)))
    rvalue))


(deftype PersistentMemoryLog [location mlog]
  Log
  (first-id-term [this]
    (first-id-term mlog))
  (last-id-term [this]
    (last-id-term mlog))
  (post-cmd! [this term rid command]
    (persist-if-changed location (.log mlog) #(post-cmd! mlog term rid command)))
  (put-cmd! [this id term rid command]
    (persist-if-changed location (.log mlog) #(put-cmd! mlog id term rid command)))
  (get-entry [this id]
    (get-entry mlog id))
  (ltrim-log! [this last-id]
    (persist-if-changed location (.log mlog) #(ltrim-log! mlog last-id)))
  (rtrim-log! [this first-id]
    (persist-if-changed location (.log mlog) #(rtrim-log! mlog first-id))))



(defn make-memory-log
  "Create a MemoryLog instance.  This is for testing purposes."
  ([] (make-memory-log []))
  ([log] (->MemoryLog (ref log))))

(defn make-persistent-log
  "Create a PersistentMemoryLog instance."
  ([location & {:keys [data] :or {data []}}]
   (->PersistentMemoryLog location (make-memory-log (util/load-obj-if-present
                                                     location :default-val data)))))



(defn- system-time-ms
  "Return the current system time, in milliseconds."
  []
  (System/currentTimeMillis))

(defn- generate-rid
  "Generates a random request ID (string)"
  []
  (str (java.util.UUID/randomUUID)))


(defn- br-get-state-machine
  "Helper function for BasicRaft that will return the requested
  state machine (by MACHINE identifier)."
  [machine servers-config server-state]
  (let [{:keys [:state :voted-for]} @server-state
        sm (filter #(= machine (.id %)) (:state-machines @servers-config))]
    (cond
      (not= state :leader) [{:status :moved :server voted-for :state nil} nil]
      (empty? sm) [{:status :not-found :server voted-for :state nil} nil]
      :else [{:status :ok :server voted-for} (first sm)])))


(defn- br-broadcast-timeout-ms
  "Extracts and returns the leader broadcast timeout (in ms) from
  the :broadcast-timeout in ELECTION-CONFIG.  If not present,
  supplies a default value of 15 ms."
  [{:keys [:broadcast-timeout] :or {:broadcast-timeout 15}}]
  broadcast-timeout)


(defn- br-election-timeout-ms
  "Computes a random election timeout between the minimum
  and maximum configuration settings."
  [{:keys [:election-timeout-min :election-timeout-max]}]
  (+ (rand-int (- election-timeout-max election-timeout-min)) election-timeout-min))


(defn- br-start-timers!
  "Start all state-specific timer(s).
  Mutates: The timers ref.
  "
  [{:keys [:election-config :server-state :timers]
    :as this}]
  (let [state (:state @server-state)]
    (condp = state
      :follower (let [follower-timer (future (Thread/sleep
                                              (br-election-timeout-ms @election-config))
                                             (candidate! this))]
                  (dosync
                   (ref-set timers {:follower follower-timer})))
      :candidate (let [candidate-timer (future (Thread/sleep
                                                (br-election-timeout-ms @election-config))
                                               (candidate! this))]
                   (dosync (ref-set timers {:candidate candidate-timer})))
      :leader (let [leader-timer (future (Thread/sleep
                                          (br-broadcast-timeout-ms @election-config))
                                         (leader! this))]
                (dosync (ref-set timers {:leader leader-timer})))
      nil)))

(defn- br-cancel-timers!
  "Cancel all timer(s)."
  ([{:keys [:timers]}]
   (try
     (dorun (map future-cancel (vals @timers)))
     (catch Exception e (logger/debugf "br-cancel-timers!: error=%s" (.getMessage e)))))
  ([{:keys [:timers]} timer]
   (try
     (dorun (map future-cancel (map second (filter #(= (first %) timer) @timers))))
     (catch Exception e (logger/debugf "br-cancel-timers!: timer=%s, error=%s" timer (.getMessage e))))))



(defn- br-candidate!
  "Possibly switch to :candidate mode.  This function handles two cases:
  1. We are using normal Raft election protocol
  2. We are using configured leader protocol.
  Normal Raft Protocol (leader in server-config is nil)
  - election timeout occurred
  - increment term counter, switch to :candidate mode, and request votes
  Configured Raft Protocol (the servers-config has a non-nil leader).
  - If either of the following is true, we switch to :candidate mode:
    - election timeout occurred and we don't have a current leader assigned
      (voted-for is nil)
    - we are configured to be the :leader but we are not
  "
  [{:keys [:election-config :id :leader-state :log :rpc :server-state :servers-config :timers]
    :as this}]
  (logger/debugf "br-candidate! id=%s, server-state=%s"
                 id @server-state)
  (let [leader (:leader @servers-config)
        voted-for (:voted-for @server-state)
        followers (disj (apply clojure.set/union
                               (:servers @servers-config)) id)]
    (cancel-timers! this)
    (if (or (nil? leader)
            (and (= id leader) (not= leader voted-for)))
      (let [new-term (inc (:current-term @server-state))]
        (dosync (alter server-state assoc
                       :state :candidate
                       :current-term new-term
                       :voted-for nil))
        (let [min-quorum (if (zero? (count followers)) 0 (quot (count followers) 2))
              [lid lterm] (last-id-term log)
              resp (request-vote rpc
                                 followers
                                 (->Vote new-term id lid lterm))
              num-success (count (filter :vote-granted (vals resp)))
              max-term (apply max (map :term (vals resp)))]
          (logger/debugf (str
                          "br-candidate! id=%s, new-term=%s, "
                          "lid=%s, lterm=%s, resp=%s")
                         id new-term lid lterm resp)

          (if (< num-success min-quorum)
            (do
              (start-timers! this)
              (when (> max-term new-term)
                (dosync (alter server-state assoc :current-term max-term))))
              (leader! this new-term))))
      (follower! this))))

(defn- br-follower!
  "Implementation of the Election protocol follower! function
  for a BasicRaft"
  [{:keys [:election-config :id :timers :server-state]
    :as this}]
  (cancel-timers! this)
  (dosync
   (alter server-state assoc :state :follower))
  (start-timers! this))


(defn- br-leader!
    "The arity/1 function trigers the leader to broadcast an
     empty append-entries RPC to all of its follows if normal
     Raft election is being used.  If configured Raft election is
     being used this is a noop.
    The arity/2 function is called when a :candidate wins an
    election and needs to transition to :leader state."

  ([{:keys [:id :log :rpc :election-config :timers
            :servers-config :server-state :leader-state]
     :as this}]
   (cancel-timers! this)
   (let [leader (:leader @election-config)
         [lid lterm] (last-id-term log)
         commit-index (:commit-index @server-state)]
     (when (or (nil? leader)
               (< commit-index lid))
       (command! this (generate-rid) nil))
     (start-timers! this)))
  ([{:keys [:id :log :rpc :election-config :timers
            :servers-config :server-state :leader-state]
     :as this} new-term]
   (cancel-timers! this)
   (dosync (alter server-state assoc
                  :current-term new-term
                  :state :leader
                  :voted-for id))
   (command! this (generate-rid) {:noop {}})
   (start-timers! this)))



(def br-basicraft-election
  "Mapping of Election protocol implementation functions
  for a BasicRaft."
  {:cancel-timers! br-cancel-timers!
   :candidate! br-candidate!
   :follower! br-follower!
   :leader! br-leader!
   :start-timers! br-start-timers!})


;;; TODO - refactor to shorten
(defn- br-append-entries
  "Implementation of Raft append-entries for a Basic Raft"
  [{:keys [:id :log :server-state :servers-config]
    :as this}
   {:keys [:term :leader-id
           :prev-log-index :prev-log-term
           :entries :leader-commit]}]
  (logger/debugf (str "br-append-entries-1: id=%s, term=%s, leader-id=%s, "
                      "prev-log-index=%s, prev-log-term=%s, "
                      "entries=%s, leader-commit=%s")
                 id term leader-id prev-log-index prev-log-term
                 entries leader-commit)

  (let [state-machines (:state-machines @servers-config)
        {:keys [:current-term :commit-index]} @server-state
        pentry (get-entry log prev-log-index)
        eentry (get-entry log (inc prev-log-index))
        [lid lterm] (last-id-term log)
        find-first-conflicting-id (fn [sidx sterm]
                                    (or
                                     (some (fn [i] (let [term (:term (get-entry log i))]
                                                     (when-not (= term sterm) (inc i))))
                                           (range sidx 0 -1))
                                     sidx))]
    (logger/debugf (str "br-append-entries-2: id=%s servers-config=%s, "
                        "server-state=%s, current-term=%s, "
                        "commit-index=%s, pentry=%s, eentry=%s, "
                        "lid=%s, lterm=%s")
                   id @servers-config @server-state
                   current-term commit-index
                   pentry eentry lid lterm)
    (cond
      (< term current-term) {:term current-term
                             :success false
                             :conflicting-term nil
                             :conflicting-first-idx nil}
      (not= lterm prev-log-term) {:term current-term
                                  :success false
                                  :conflicting-term lterm
                                  :conflicting-first-idx (find-first-conflicting-id lid lterm)}
      :else (do
              (when (and eentry (not= (:term eentry) term))
                (rtrim-log! log (inc prev-log-index)))
              (doseq [{:keys [:id :term :rid :command]} entries]
                (put-cmd! log id term rid command))
              (let [[last-new-idx _] (last-id-term log)
                    new-current-term term
                    new-commit-index (if (> leader-commit commit-index)
                                       (min leader-commit last-new-idx)
                                       commit-index)]
                (dosync (alter server-state assoc
                               :current-term new-current-term
                               :voted-for leader-id
                               :last-leader-cmd-time (system-time-ms)
                               :commit-index new-commit-index))
                (when (not= (:state @server-state) :follower)
                  (follower! this))
                (logger/debugf "br-append-entries-3: id=%s, server-state=%s"
                               id @server-state)
                (dorun
                 (pmap (fn [s]
                         (process-log! s log servers-config server-state)) state-machines))
                {:term new-current-term
                 :success true
                 :conflicting-term nil
                 :conflicting-first-idx nil})))))


(defn- br-command!
  "Send a command to the leader of the state machine.  If COMMAND is nil,
  the system will issue an 'empty' command, like what the leader does when
  it broadcasts heartbeats. This does not get stored in the log."
  [{:keys [:id :log :rpc :server-state :servers-config]
    :as this}
   rid command]
  (let [{:keys [:commit-index :current-term :state :voted-for]} @server-state
        state-machines (:state-machines @servers-config)
        followers (disj (apply clojure.set/union (:servers @servers-config)) id)
        ;; we implicitly include ourself in the quorum, so this is just the remaining
        ;; quorum we need to commit a log entry
        min-quorum (if (zero? (count followers)) 0 (quot (count followers) 2))]
    (logger/debugf (str "command!-1: id=%s,  commit-index=%s, current-term=%s, state=%s, "
                        "voted-for=%s, followers=%s, min-quorum=%s")
                   id commit-index current-term
                   state voted-for followers min-quorum)
    (cond
      (= state :leader) (try
                          (let [[pidx pterm] (last-id-term log)
                                new-id (if (nil? command) pidx (post-cmd! log current-term rid command))
                                _ (dorun (pmap #(process-log! %1 log servers-config server-state)
                                               state-machines))
                                cmd (if (nil? command) []
                                        [{:id new-id :term current-term
                                          :rid rid :command command}])
                                resp (append-entries rpc
                                                     followers
                                                     (->Entry current-term id pidx pterm
                                                              cmd
                                                              commit-index))
                                num-success (count (filter :success (vals resp)))
                                max-term (apply max (cons current-term (map :term (remove :success (vals resp)))))]
                            (logger/debugf "command!-2: id=%s, num-success=%s, max-term=%s, resp=%s"
                                           id num-success max-term resp)
                            (cond
                              (< current-term max-term) (let [[sid _] (first (filter (fn [[s {:keys [:success :term]}]]
                                                                                       (and (not success) (= term max-term)))
                                                                                     resp))]
                                                          (follower! this)
                                                          {:status :moved :server sid})
                              (< num-success min-quorum) {:status :unavailable :server id}
                              :else (dosync
                                     (alter server-state assoc :commit-index new-id)
                                     {:status :accepted :server id})))
                          (catch IllegalArgumentException iae {:status :conflict :server id}))
      :else {:status :moved :server voted-for})))


(defn- br-get-machine-state
  "Implementation of Raft protocol get-machine-state function for
  a BasicRaft"
  ([{:keys [:server-state :servers-config]}
    machine]
   (let [[resp sm] (br-get-state-machine machine
                                         servers-config
                                         server-state)]
     (if (= (:status resp) :ok)
       (assoc resp :state (get-state sm))
       resp)))
  ([this machine resource-id]
   (get-machine-state this machine resource-id nil))
  ([{:keys [:server-state :servers-config]} machine resource-id default]
   (let [[resp sm] (br-get-state-machine machine
                                         servers-config
                                         server-state)]
     (if (= (:status resp) :ok)
       (assoc resp :state (get-state sm resource-id default))
       resp))))


(defn- br-install-snapshot
  "TODO - Implementation of the Raft protocol install-snapshot
  function for a BasicRaft."
  [{:keys [:id :log :rpc :election-config :timers
           :servers-config :server-state :leader-state]
    :as this}
   snapshot]
  (throw (Exception. "TODO - Implement install-snapshot")))


(defn- br-request-vote
  "Process a request to grant leadership to a new server.  This method handles both
  a traditional Raft election mechanism and a configured-leader election.
  "
  [{:keys [:id :log :rpc :election-config :timers
           :servers-config :server-state :leader-state]
    :as this}
   {:keys [:term :candidate-id :last-log-index :last-log-term]}]
  (let [{:keys [:current-term :voted-for :last-leader-cmd-time] :or {:last-leader-cmd-time 0}} @server-state
        {:keys [:leader]} @servers-config
        {:keys [:election-timeout-min] :or {:election-timeout-min 0}} election-config
        [lid lterm] (last-id-term log)
        leader-cmd-time-elapsed (- (system-time-ms) last-leader-cmd-time)]
    (logger/debugf (str
                    "br-request-vote: id=%s, term=%s, candidate-id=%s, "
                    "last-log-index=%s, last-log-term=%s, current-term=%s, "
                    "last-leader-cmd-time=%s, voted-for=%s, lid=%s, lterm=%s, "
                    "leader=%s, leader-cmd-time-elapsed=%s")
                   id term candidate-id last-log-index last-log-term
                   current-term last-leader-cmd-time voted-for lid lterm
                   leader leader-cmd-time-elapsed)
    (if (and (<= current-term term)
             (or (< lterm last-log-term) (and (= lterm last-log-term)
                                              (<= lid last-log-index)))
             (or (= candidate-id leader)
                 (and (or (nil? voted-for)
                          (= voted-for candidate-id))
                      (< election-timeout-min leader-cmd-time-elapsed))))
      (do (dosync (alter server-state assoc
                         :current-term term
                         :voted-for candidate-id))
          {:term term :vote-granted true})
      {:term current-term :vote-granted false})))


(def br-basicraft-raftprotocol
  "Mapping of Raft protocol functions for a BasicRaft"
  {:append-entries br-append-entries
   :command! br-command!
   :get-machine-state br-get-machine-state
   :install-snapshot br-install-snapshot
   :request-vote br-request-vote
   })


;;; Basic Raft Implementation
;;;
;;; id - the ID of this server [static]
;;; log - a Log implementation [static]
;;; rpc - an RPC implementation that knows how to communicate will all servers (by their
;;;       ID) in the union of all the server-sets in the servers vector.
;;;       The RPC must implement the three-argument arity of all of the functions defined
;;;       in the RaftProtocol
;;; election-config - a map with the following entries [static]
;;;   :broadcast-timeout - time in ms for leader broadcast (if nil, no broadcasts are issued)
;;;   :election-timeout-min - minimum time in ms for election timeout
;;;   :election-timeout-max - maximum time in ms for election timeout
;;; timers - a map (wrapped in a [ref]) whose keys are timer keywords and whose
;;;    values are functions (run in a future).  They all take a callback function
;;;    that is part of the Election protocol if they timeout.
;;;    keys include:
;;;      :follower  (follower-state election timeout)
;;;      :candidate (candidate-state timeout)
;;;      :broadcast (leader-state send-broadcast timeout)
;;; servers-config - a map with the following entries [ref]
;;;   :servers - a vector that contains one or two sets of server-ids for all
;;;             servers in the Raft cluster.  if there is only one entry in the
;;;             servers vector that is the current config
;;;             If there are two entries in the servers vector, the first
;;;             entry is the current config and the second entry is the new
;;;             config that we are transitioning to
;;;   :state-machines - a sequence of state machine instances
;;;   :leader - if not nil, using configured leader election protocol.  The value
;;;     should be the ID of ther server that is supposed to be the leader.
;;;
;;; server-state - a map that is persisted automatically as values are changed. [ref]
;;;   :state - :follower, :candidate, :leader
;;;   :current-term - latest term this server has seen.  initialized to 0 on first boot
;;;   :voted-for - candidate that received vote in current term (null if none)
;;;   :commit-index - index of highest log entry known to be committed.  Initialized
;;;     to 0, increases monotonically
;;;   :last-applied - map
;;;     keys = <state-machine-identifier>
;;;     values = map whose key is the state machine command (e.g., :patch)
;;;              and whose values are the highest log entry applied to the state
;;;              machine for that command.  They start at 0 and increase
;;;              monotonically. TODO - Restriction: Each state machine should operate
;;;              on just one type of command.  Multiple state machines MAY process the
;;;              same command.  That's OK, they will have different <state-machine-identifiers>
;;;              When this is implementerd, values are just integers that reflect the latest
;;;              log entry that was processed by the state machine.
;;;   :last-leader-cmd-time - system time in ms when last cmd received from leader.
;;;      See spec, section 6, last paragraph.
;;; leader-state - a map that is persisted automatically as values are changed.  used only
;;;     if state = :leader [ref]
;;;   keys: <server-ids>
;;;   values: {
;;;     next-index <int>
;;;     match-index <int>
;;;   }
;;;   next-index is the index of the next log entry to send to that server
;;;   match-index is the index of the highest log entry known to be
;;;     replicated to that server.

(defrecord BasicRaft [id                ; server-id
                      log               ; Log instance
                      rpc               ; RPC instance
                      election-config   ; map
                      timers            ; map (ref)
                      servers-config    ; map (ref)
                      server-state      ; map (ref)
                      leader-state])    ; map (ref)


(extend BasicRaft
  Election
  br-basicraft-election
  RaftProtocol
  br-basicraft-raftprotocol
  )

(actor-wrapper BasicRaftActor [Election RaftProtocol] defrecord)


(defn make-basic-raft
  "Constructs a BasicRaft instance that has been actor-fied.
   Parameters:
     id - the server id
     log - a Log instance
     rpc - an RPC instance (basically anything that implements the RaftProtocol
     election-config - a map with the following entries [static]
       :broadcast-timeout - time in ms for leader broadcast
       :election-timeout-min - minimum time in ms for election timeout
       :election-timeout-max - maximum time in ms for election timeout
  "
  [id log rpc election-config servers-config server-state leader-state]
  (->BasicRaftActor (chan) (BasicRaft.
                            id
                            log
                            rpc
                            election-config
                            (ref {})
                            (ref servers-config)
                            (ref server-state)
                            (ref leader-state))))


(defmulti make-raft
  (fn [cfg rpc] (:cluster-raft (:local cfg)))
  :default :memory)

;; (defmethod make-raft :memory [{:keys [:local] :as cfg} rpc]
;;   (let [id (config/local-node-id cfg)
;;         log (make-memory-log)]))
