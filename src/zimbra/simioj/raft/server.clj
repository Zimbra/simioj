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

(def DOWN-FOLLOWER-RETRY-DEF
  "Default value (in milliseconds) of :down-follower-retry if not specified in :servers-config"
  100)


(def MAX-ENTRIES-DEF
  "Default value of :max-entries if not specified in :servers-config"
  5)

(defrecord Entry [^Number term            ; leader's term
                  leader-id               ; leader's ID so followers can redirect
                  ^Number prev-log-index  ; idx of log entry immediatly preceeding
                  ^Number prev-log-term   ; term of prev-log-index entry
                  entries                 ; entries (commands) to store (empty for heartbeat)
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
;;;; Server, Election Protocol
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Server, Raft Protocol
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


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
      RID - (str) A client-generated request ID.  Every command submitted
        to the system must have a unique request ID.  It is recommended
        that the clients use either a UUID or a value of the form
        <client-id>-<request-serial-number> or some other construction
        that is guaranteed to be unique.  If a new command is submitted to
        the system that uses the same RID as a previous command, the
        system will not store the new command.
      COMMAND ???
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
                :payload {}}]
       Instructions to the leader for changing the set of servers that belong
       in the Raft are done via a special command.  A :set-config command is
       sent to the leader.  It should have this form:
         [:set-config [<new-server-identifiers-set>]]
       Prior to storing this log entry, the :leader must process it as follows:

       Compare it's current :servers set with the new servers set.  If they
          are different:
       1. Update the :set-config command as follows:
          [:set-config [<current-server-identifiers-set> <new-server-identifiers-set>]]
       2. It updates its current :servers with the new value and stores it in the log.
       3. It replicates the log entry to the followers using the union of the two sets
          for concensus.
       4. As soon as that log entry has been committed, the leader should
          then construct a new :set-config command itself and store/replicate.  This
          new command would look like this:
          [:set-config [<new-server-identifiers-set>]]
       5. It would update its own :server-state immediately and replicate it.
       NOTE: Configuration updates are apply by server immediately upon
             receipt, before committing.  The values in the vector are sets of server
             ids for all servers that are members of the Raft cluster.  During
             normal operations there will be only one value in the vector. During
             a topology change there will be two values.
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
    SERVER-IDS is a seq of one or more server IDs.
    The first form should return a map
      {:term <current-term>
       :success <boolean>
       ;; These used to speed repair.  If :success is true, the values
       ;; are meaningless and are not used.
       :conflicting-term <conflicting-term-number>
       :conflicting-first-idx <first-log-index-containing-conflicting-term>}
    The second form should return a map of the following form.  Note that the RPC
    implementation may return `nil` as the value for a given <server-id> if that
    server could not be contacted.
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
;;;; StateMachine functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- rsm-process-set-config! [{:keys [:server-state] :as this} command]
  (if (and command
           (= (first command) :set-config))
    (let [old-servers (:servers @server-state)
          new-servers (second command)
          state-changed? (not= old-servers new-servers)]
      (when state-changed?
        (dosync (alter server-state assoc :servers new-servers))
        (notify-state-changed this [:config :set-config] old-servers new-servers))
      {:applied? true
       :state-changed? state-changed?
       :old-state old-servers
       :new-state new-servers})
    {:applied? false
     :state-changed? false}))


(defn- rsm-process-command! [{:keys [:servers-config] :as this} command]
  (if command
    (let [cmd-id (first command)
          {:keys [:state-processors] :or {:state-processors {}}} servers-config
          updated (filter
                   (fn [[spid spr]] (:state-changed? spr))
                   (map (fn [[spid spi]] [spid (process-command! spi command)]) state-processors))
          state-changed? (not-empty updated)
          old-state (into {} (map (fn [[spid spr]] [spid (:old-state spr)]) updated))
          new-state (into {} (map (fn [[spid spr]] [spid (:new-state spr)]) updated))]
      (dorun (map (fn [[spid spr]] (notify-state-changed this [spid cmd-id] (:old-state spr) (:new-state spr)))
                  updated))
      {:applied? true
       :state-changed? state-changed?
       :old-state old-state
       :new-state new-state})
    {:applied? false
     :state-changed? false}))


(defn- rsm-get-state
  "Since this high-level StateMachine may contain multiple embedded state machine
  processors, each with their own state-machine-processor-id, the RESOURCE-ID argument
  is expectd to be a vector of the form [<state-machine-processor-id> <resource-id>]."
  ([{:keys [:servers-config] :as this}]
   (let [{:keys [:state-processors] :or {:state-processors {}}} servers-config]
     (into {} (map (fn [[spid spi]] [spid (get-state spi)]) state-processors))))
  ([this resource-id] (get-state this resource-id nil))
  ([{:keys [:servers-config] :as this} [spid resource-id] default]
   (let [{:keys [:state-processors] :or {:state-processors {}}} servers-config
         spi (state-processors spid)]
     (if spi (get-state spi resource-id default)
         default))))


(defn- rsm-notify-state-changed [{:keys [:servers-config] :as this}
                                 [spid command-id] old-state new-state]
  (let [{:keys [:state-change-listeners] :or {:state-change-listeners {}}} servers-config
        notify-fns (state-change-listeners spid [])]
    (dorun (map #(% command-id old-state new-state) notify-fns))))


(def rsm-state-machine
  "Map of StateMachine functions for the RaftStateMachine"
  {:process-set-config! rsm-process-set-config!
   :process-command! rsm-process-command!
   :get-state rsm-get-state})

(def rsm-state-changed-notifier
  "Map of StateChangedNotifier functions for the RaftStateMachine"
  {:notify-state-changed rsm-notify-state-changed})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Raft Server Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn system-time-ms
  "Return the current system time, in milliseconds."
  []
  (System/currentTimeMillis))

(defn generate-rid
  "Generates a random request ID (string)"
  []
  (str (java.util.UUID/randomUUID)))


(defn- rs-process-log!
  "Apply all committed log entries to the state machine, starting
  with :last-applied + 1."
  [{:keys [:log :rpc :election-config :timers
           :servers-config :server-state :leader-state]
    :as this}]
  (let [{:keys [:last-applied :commit-index] :or {:last-applied 0 :commit-index 0}} @server-state]
    (loop [next-to-apply (inc last-applied)
           entry (get-entry log next-to-apply)]
      (if (and (<= next-to-apply commit-index)
               (some? entry))
        (do
          (process-command! this (:command entry))
          (recur (inc next-to-apply) (get-entry log (inc next-to-apply))))
        (dosync (alter server-state assoc :last-applied (dec next-to-apply)))))))


(defn- rs-broadcast-timeout-ms
  "Extracts and returns the leader broadcast timeout (in ms) from
  the :broadcast-timeout in ELECTION-CONFIG.  If not present,
  supplies a default value of 15 ms."
  [{:keys [:broadcast-timeout] :or {:broadcast-timeout 15}}]
  broadcast-timeout)


(defn- rs-follower-ids
  "Computes the union of all of the sets in the :servers
  vector in the SERVER-STATE ref.
  Returns a set"
  [{:keys [:id :server-state]}]
  (disj (apply clojure.set/union (:servers @server-state)) id))


(defn- rs-current-follower-ids
  "Filters out any followers that have an active (non-nil) :updater
  in :leader-state.
  Returns a set
  "
  [{:keys [:leader-state] :as this}]
  (clojure.set/difference (rs-follower-ids this)
                          (map first (filter (comp :updater second) @leader-state))))


(defn- rs-election-timeout-ms
  "Computes a random election timeout between the minimum
  and maximum configuration settings."
  [{:keys [:election-timeout-min :election-timeout-max] :or {:election-timeout-min 150 :election-timeout-max 300}}]
  (let [tout (+ (rand-int (- election-timeout-max election-timeout-min)) election-timeout-min)]
    (logger/tracef "rs-election-timeout-ms: tout=%s" tout)
    tout))


(defn- rs-start-timers!
  "Start all state-specific timer(s).
  Mutates: The timers ref.
  "
  [{:keys [:id :election-config :server-state :timers]
    :as this}]
  (logger/tracef "rs-start-timers!: id=%s, election-config=%s, state=%s"
                 id election-config (:state @server-state))
  (let [state (:state @server-state)]
    (condp = state
      :follower (let [follower-timer (future (Thread/sleep (rs-election-timeout-ms election-config))
                                             (try
                                               (candidate! this)
                                               (catch InterruptedException ie
                                                 (logger/tracef "follower election timeout exception: id=%s, ex=%s" id ie))
                                               (catch Exception e
                                                 (logger/warnf e "follower election exception: id=%s, ex=%s" id e))))]
                  (dosync
                   (ref-set timers {:follower follower-timer})))
      :candidate (let [candidate-timer (future (Thread/sleep (rs-election-timeout-ms election-config))
                                               (try
                                                 (candidate! this)
                                               (catch InterruptedException ie
                                                 (logger/tracef "candidate election timeout exception: id=%s, ex=%s" id ie))
                                                 (catch Exception e
                                                   (logger/warnf e "candidate election exception: id=%s, ex=%s" id e))))]
                   (dosync (ref-set timers {:candidate candidate-timer})))
      :leader (let [leader-timer (future (Thread/sleep (rs-broadcast-timeout-ms election-config))
                                         (try
                                           (leader! this)
                                           (catch InterruptedException ie
                                             (logger/tracef "leader broadcast timeout exception: id=%s, ex=%s" id ie))
                                           (catch Exception e
                                             (logger/warnf e "leader broadcast exception: id=%s, ex=%s" id e))))]
                (dosync (ref-set timers {:leader leader-timer})))
      nil)))

(defn- rs-cancel-timers!
  "First form cancels all timer(s).  Second form just cancels specific timer."
  ([{:keys [:timers]}]
   (try
     (dorun (map future-cancel (vals @timers)))
     (catch Exception e (logger/errorf "rs-cancel-timers!/1: error=%s" (.getMessage e)))))
  ([{:keys [:timers]} timer]
   (try
     (dorun (map future-cancel (map second (filter #(= (first %) timer) @timers))))
     (catch Exception e (logger/errorf "rs-cancel-timers!/2: timer=%s, error=%s" timer (.getMessage e))))))



(defn- rs-candidate!
  "Switch to :candidate mode.
  - election timeout occurred
  - increment term counter, switch to :candidate mode, and request votes
  "
  [{:keys [:election-config :id :leader-state :log :rpc :server-state :servers-config :timers]
    :as this}]
  ;; (cancel-timers! this)
  (logger/tracef "rs-candidate!: id=%s, server-state=%s" id @server-state)
  (let [leader (:leader @server-state)
        voted-for (:voted-for @server-state)
        followers (rs-follower-ids this)
        followers? (pos? (count followers))]
    (let [new-term (inc (:current-term @server-state 0))]
      (dosync (alter server-state assoc
                     :state :candidate
                     :current-term new-term
                     :voted-for nil))
      (let [min-quorum (if followers? (quot (count followers) 2) 0)
            [lid lterm] (last-id-term log)
            resp (request-vote rpc
                               followers
                               (->Vote new-term id lid lterm))
            _ (logger/debugf "rs-candidate! id=%s, request-vote-resp=%s" id resp)
            num-success (if followers? (count (filter :vote-granted (vals resp))) 0)
            max-term (if followers? (apply max (map :term (vals resp))) 0)]
        (logger/tracef "rs-candidate!: id=%s, new-term=%s, lid=%s, lterm=%s, num-succes=%s, max-term=%s, resp=%s"
                       id new-term lid lterm num-success max-term resp)
        (if (< num-success min-quorum)
          (do
            (when (> max-term new-term)
              (dosync (alter server-state assoc :current-term max-term)))
            (follower! this))
          (leader! this new-term))))))

(defn- rs-follower!
  "Implementation of the Election protocol follower! function
  for a RaftServer"
  [{:keys [:election-config :id :log :timers :servers-config :server-state]
    :as this}]
  (cancel-timers! this)
  (let [leader (:leader @server-state)]
    (logger/tracef "rs-follower!: id=%s, leader=%s, server-state=%s, last-id-term=%s"
                   id leader @server-state (last-id-term log))
    (if (and (= leader id)
             (not= (:state @server-state) :leader))
      (candidate! this)
      (do
        (dosync
         (alter server-state assoc :state :follower))
        (start-timers! this)))))


(defn- rs-leader!
  "The arity/1 function trigers the leader to broadcast an
     empty append-entries RPC to all of its follows if normal
     Raft election is being used.  If configured Raft election is
     being used this is a noop.
    The arity/2 function is called when a :candidate wins an
    election and needs to transition to :leader state."

  ([{:keys [:id :log :rpc :election-config :timers
            :servers-config :server-state :leader-state]
     :as this}]
   ;;(cancel-timers! this)
   (logger/tracef "rs-leader!/1: id=%s, server-state=%s, leader-state=%s"
                  id @server-state @leader-state)
   (let [[lid lterm] (last-id-term log)
         commit-index (:commit-index @server-state)
         resp (command! this (generate-rid) nil)]
     (logger/tracef "rs-leader!/1: id=%s, resp=%s" id resp)
     (when (= (:status resp) :accepted)
       (start-timers! this))))
  ([{:keys [:id :log :rpc :election-config :timers
            :servers-config :server-state :leader-state]
     :as this} new-term]
   (let [[lid lterm] (last-id-term log)]
     (logger/tracef "rs-leader!/2: id=%s, new-term=%s, server-state=%s, leader-state=%s"
                    id new-term @server-state @leader-state)
     (dosync (alter server-state assoc
                    :current-term new-term
                    :state :leader
                    :voted-for id)
             (ref-set leader-state
                      (into {} (map (fn [sid] [sid {:next-index (inc lid) :match-index 0 :updater nil}])
                                    (rs-follower-ids this)))))
     (let [resp (command! this (generate-rid) nil)]
       (logger/tracef "rs-leader!/2: id=%s, resp=%s" id resp)
       (if (= (:status resp) :accepted)
         (start-timers! this)
         (follower! this))))))


(def rs-basicraft-election
  "Mapping of Election protocol implementation functions
  for a RaftServer."
  {:cancel-timers! rs-cancel-timers!
   :candidate! rs-candidate!
   :follower! rs-follower!
   :leader! rs-leader!
   :start-timers! rs-start-timers!})


;;; TODO - refactor to shorten
(defn- rs-append-entries
  "Implementation of Raft append-entries for a RaftServer"
  [{:keys [:id :log :server-state :servers-config]
    :as this}
   {:keys [:term :leader-id
           :prev-log-index :prev-log-term
           :entries :leader-commit]}]
  (cancel-timers! this)
  (logger/tracef (str "rs-append-entries-1: id=%s, term=%s, leader-id=%s, "
                      "prev-log-index=%s, prev-log-term=%s, "
                      "entries=%s, leader-commit=%s")
                 id term leader-id prev-log-index prev-log-term
                 entries leader-commit)

  (let [{:keys [:current-term :commit-index] :or {:current-term 0 :commit-index 0}} @server-state
        pentry (get-entry log prev-log-index)
        eentry (get-entry log (inc prev-log-index))
        [lid lterm] (last-id-term log)
        find-first-conflicting-id (fn [sidx sterm]
                                    (or
                                     (some (fn [i] (let [term (:term (get-entry log i))]
                                                     (when-not (= term sterm) (inc i))))
                                           (range sidx 0 -1))
                                     sidx))
        _ (logger/tracef (str "rs-append-entries-2: id=%s servers-config=%s, "
                              "server-state=%s, current-term=%s, "
                              "commit-index=%s, pentry=%s, eentry=%s, "
                              "lid=%s, lterm=%s")
                         id servers-config @server-state
                         current-term commit-index
                         pentry eentry lid lterm)
        rc (cond
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
                       (logger/tracef "rs-append-entries-3: id=%s, server-state=%s"
                                      id @server-state)
                       (rs-process-log! this)
                       {:term new-current-term
                        :success true
                        :conflicting-term nil
                        :conflicting-first-idx nil})))]
    (follower! this)
    rc))



(defn- rs-compute-entries-for-follower
  "Returns an Entry that contains the maximum number of commands allowed
  based on the term of the log entry in FIRST-INDEX and the max-entries
  config value.
  Parameters:
  LOG - the log instance
  MAX-ENTRIES - the max number of entries to send in a single append-entries rpc call.
    All must belong to the same term.
  LEADER-ID - the id of the leader issuing the append-entries rpc
  LEADER-NEXT-INDEX - the leaders next-index value
  LEADER-TERM - the leader's current term
  LEADER-COMMIT-INDEX - the leaders current commit-index
  PREV-LOG-INDEX - the previous log index
  PREV-LOG-TERM - the previous log term
  FIRST-INDEX - the index of the first entry to send.
  "
  [log max-entries leader-id leader-next-index leader-term leader-commit-index
   prev-log-index prev-log-term first-index]
  (logger/tracef (str "rs-compute-entries-for-follower: max-entries=%s, leader-id=%s, "
                     "leader-next-index=%s, leader-term=%s, leader-commit-index=%s, "
                     "prev-log-index=%s, prev-log-term=%s, first-index=%s")
                max-entries leader-id, leader-next-index leader-term
                leader-commit-index prev-log-index prev-log-term first-index)
  (loop [rentries max-entries
         entry (get-entry log first-index)
         lterm (:term (or entry {}))
         entries []]
    (logger/tracef (str "rs-compute-entries-for-follower: rentries=%s, "
                       "entry=%s, lterm=%s, (count entries)=%s")
                  rentries entry lterm (count entries))
    (if (and (pos? rentries) entry (= (:term entry) lterm) (< (:id entry) leader-next-index))
      (recur (dec rentries) (get-entry log (inc (:id entry))) lterm (conj entries entry))
      (->Entry leader-term leader-id prev-log-index prev-log-term entries leader-commit-index))))


(defn- rs-update-follower
  "This function is run in its own thread and is responsible for bringing a single
  lagging follower up-to-date.
  Parameters:
  this - RaftServer instance
  follower-id - The ID of the follower that is behind
  "
  [{:keys [:id :leader-state :log :rpc :server-state :servers-config] :as this} follower-id]
  ;; Lookup :next-index and :match-index for follower
  ;; Compare :match-index with our (leader's (inc last-log-index)
  ;;   If the same, we are done.  Just update leader-state to set
  ;;     :updater to nil and exit.
  ;;   If follower is still behind:
  ;;     Examine follow's :match-index (it may be 0) and look up
  ;;      the previous log id and term
  ;;     Compute the next set of log commands that we can send.
  ;;     Send the log entries to that follower and update
  ;;       the :leader-state entry
  (loop [leader-next-index (inc (first (last-id-term log)))
         {:keys [:match-index :next-index] :or {:match-index 0 :next-index 1}}
         (follower-id @leader-state {})]
    (logger/tracef (str "rs-update-follower: id=%s, follower-id=%s, leader-next-index=%s, "
                       "match-index=%s, next-index=%s")
                  id follower-id leader-next-index match-index next-index)
    (if (= leader-next-index next-index)
      (dosync (alter leader-state assoc-in [follower-id :updater] nil))
      (let [{prev-log-index :id prev-log-term :term} (or (get-entry log match-index)
                                                         {:id 0 :term 0})
            entry (rs-compute-entries-for-follower log
                                                   (:max-entries @server-state MAX-ENTRIES-DEF)
                                                   id
                                                   leader-next-index
                                                   (:current-term @server-state)
                                                   (:commit-index @server-state)
                                                   prev-log-index
                                                   prev-log-term
                                                   (inc match-index))
            follower-resp (follower-id (append-entries rpc #{follower-id} entry))
            _ (logger/tracef "rs-update-follower: id=%s, follower-id=%s, follower-resp=%s"
                            id follower-id follower-resp)
            [done? new-state] (cond
                                ;; (1) Server was down, have to sleep and retry
                                (nil? follower-resp) (do
                                                       (Thread/sleep (:down-follower-retry @servers-config DOWN-FOLLOWER-RETRY-DEF))
                                                       [false :leader])
                                ;; (2) We are no longer the leader
                                (and (not (:success follower-resp))
                                     (< (:current-term @server-state)
                                        (:term follower-resp))) [true :follower]
                                ;; (3) Entries rejected because we didn't compute things right
                                (not (:success follower-resp))
                                (do
                                  (dosync (alter leader-state
                                                 update follower-id
                                                 assoc
                                                 :next-index (:conflicting-first-idx follower-id 0)
                                                 :match-index
                                                 (max 0 (dec (:conflicting-first-idx follower-id 0)))))
                                  [false :leader])
                                ;; (4) Successful
                                :else (do
                                        (dosync (alter leader-state
                                                       update follower-id
                                                       assoc
                                                       :match-index (+ (:prev-log-index entry) (count (:entries entry)))
                                                       :next-index (inc (+ (:prev-log-index entry) (count (:entries entry))))))
                                        (if (= (get-in @leader-state [follower-id :next-index] 0)
                                               (first (last-id-term log)))
                                          [true :leader]
                                          [false :leader])))]
        (if done?
          (if (= new-state :leader)
            (dosync (alter leader-state
                           assoc-in [follower-id :updater] nil))
            (follower! this))
          (recur (inc (first (last-id-term log)))
                 (follower-id @leader-state {})))))))


(defn- rs-process-append-entries!
  "Inspect the response from append-entries and do the following:
   1. update :server-state
   2. if necessary, spawn updaters for any followers that are behind.
   3. if necessary, reset :updaters for all followers that are current.
  Parameters:
   this - RaftServer
   commit-index - the new commit index
   resp - the append-entries RPC response. Will be an empty map if there are
     no current followers.
  Returns: n/a
  "
  [{:keys [:leader-state :server-state] :as this} commit-index resp]
  (let [all-followers (rs-follower-ids this)
        successful (into {} (filter (comp :success second) resp))
        failed (into {} (remove (comp :success second) resp))
        leader-state-new (reduce
                          (fn [ls [sid sval]] ;; update state of lagging followers
                            (assoc ls sid {:next-index (:conflicting-first-idx sval 0)
                                           :match-index (max 0 (dec (:conflicting-first-idx sval 0)))
                                           :updater (future (rs-update-follower this sid))}))
                          (reduce
                           (fn [ls sid] ;; update state of current follows
                             (assoc ls sid {:next-index (inc commit-index)
                                            :match-index commit-index
                                            :updater nil}))
                           @leader-state
                           (keys successful))
                          failed)]
    (dosync (alter server-state assoc :commit-index commit-index)
            (ref-set leader-state leader-state-new))))


(defn- rs-command!
  "Send a command to the leader of the state machine.  If COMMAND is nil,
  the system will issue an 'empty' command, like what the leader does when
  it broadcasts heartbeats. This does not get stored in the log.
  Returns a map that contains the following at a minimum:
    {:status <status>}
  "
  [{:keys [:id :log :rpc :server-state :servers-config]
    :as this}
   rid command]

  (let [{:keys [:commit-index :current-term :state :voted-for] :or
         {:commit-index 0 :current-term 0}} @server-state
        all-followers (rs-follower-ids this)
        followers (rs-current-follower-ids this)
        ;; we implicitly include ourself in the quorum, so this is just the remaining
        ;; quorum we need to commit a log entry
        min-quorum (quot (inc (count all-followers)) 2)]

    (logger/tracef (str "rs-command!-1: id=%s, command?=%s, commit-index=%s, "
                       "current-term=%s, state=%s, voted-for=%s, min-quorum=%s, "
                       "all-followers=%s, followers=%s")
                  id (some? command) commit-index current-term
                  state voted-for min-quorum all-followers followers)
    (cond
      (= state :leader) (try
                          (let [[pidx pterm] (last-id-term log)
                                new-id (if (nil? command) pidx
                                           (post-cmd! log current-term rid command)) ;; TODO-GOT
                                {:keys [:applied?]} (process-set-config! this command)
                                cmd (if (nil? command) []
                                        [{:id new-id :term current-term
                                          :rid rid :command command}])
                                resp (if (empty? followers) {}
                                         (append-entries
                                          rpc
                                          followers
                                          (->Entry current-term id pidx pterm
                                                   cmd
                                                   commit-index)))
                                _ (logger/tracef "rs-command!-2: id=%s, append-entries resp=%s" id resp)
                                num-success (count (filter :success (vals resp)))
                                max-term (apply max (cons current-term
                                                          (map :term (remove :success (vals resp)))))]
                            (cond
                              (< current-term max-term) (let [[sid _]
                                                              (first (filter
                                                                      (fn [[s {:keys [:success :term]}]]
                                                                        (and (not success) (= term max-term)))
                                                                      resp))]
                                                          (follower! this)
                                                          {:status :moved :server sid})
                              (< num-success min-quorum) {:status :unavailable :server id}
                              :else (do
                                      (rs-process-append-entries! this new-id resp)
                                      ;;(dosync (alter server-state assoc :commit-index new-id))
                                      (when (and command (not applied?)) (process-command! this command))
                                      {:status :accepted :server id})))
                          (catch IllegalArgumentException iae
                            (logger/warnf iae "rs-command!: id=%s Exception: %s" id iae)
                            {:status :conflict :server id}))
      :else {:status :moved :server voted-for})))


(defn- rs-get-machine-state-helper
  [processor servers-config server-state]
  (let [{:keys [:state :voted-for]} @server-state
        sm ((:state-processors servers-config) processor)]
    (cond
      (not= state :leader) [{:status :moved :server voted-for :state nil} nil]
      (nil? sm) [{:status :not-found :server voted-for :state nil} nil]
      :else [{:status :ok :server voted-for} sm])))


(defn- rs-get-machine-state
  "Implementation of Raft protocol get-machine-state function for
  a RaftServer"
  ([{:keys [:server-state :servers-config]}
    processor]
   (let [[resp sm] (rs-get-machine-state-helper processor
                                                servers-config
                                                server-state)]
     (if (= (:status resp) :ok)
       (assoc resp :state (get-state sm))
       resp)))
  ([this processor resource-id]
   (get-machine-state this processor resource-id nil))
  ([{:keys [:server-state :servers-config]} processor resource-id default]
   (let [[resp sm] (rs-get-machine-state-helper processor
                                                servers-config
                                                server-state)]
   (if (= (:status resp) :ok)
     (assoc resp :state (get-state sm resource-id default))
     resp))))


(defn- rs-install-snapshot
  "TODO - Implementation of the Raft protocol install-snapshot
  function for a RaftServer."
  [{:keys [:id :log :rpc :election-config :timers
           :servers-config :server-state :leader-state]
    :as this}
   snapshot]
  (throw (Exception. "TODO - Implement install-snapshot")))


(defn- rs-request-vote
  "Process a request to grant leadership to a new server.  This method handles both
  a traditional Raft election mechanism and a configured-leader election.
  "
  [{:keys [:id :log :rpc :election-config :timers
           :servers-config :server-state :leader-state]
    :as this}
   {:keys [:term :candidate-id :last-log-index :last-log-term]}]
  (let [{:keys [:current-term :leader :voted-for :last-leader-cmd-time] :or {:current-term 0 :last-leader-cmd-time 0}} @server-state
        {:keys [:election-timeout-min] :or {:election-timeout-min 0}} election-config
        [lid lterm] (last-id-term log)
        leader-cmd-time-elapsed (- (system-time-ms) last-leader-cmd-time)
        _ (logger/debugf (str
                          "rs-request-vote: id=%s, term=%s, candidate-id=%s, "
                          "last-log-index=%s, last-log-term=%s, current-term=%s, "
                          "last-leader-cmd-time=%s, voted-for=%s, lid=%s, lterm=%s, "
                          "leader=%s, leader-cmd-time-elapsed=%s")
                         id term candidate-id last-log-index last-log-term
                         current-term last-leader-cmd-time voted-for lid lterm
                         leader leader-cmd-time-elapsed)
        resp (if (and (<= current-term term)
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
               {:term current-term :vote-granted false})]
    (logger/debugf "rs-request-vote: id=%s, resp=%s" id resp)
    resp))



(def rs-basicraft-raftprotocol
  "Mapping of Raft protocol functions for a RaftServer"
  {:append-entries rs-append-entries
   :command! rs-command!
   :get-machine-state rs-get-machine-state
   :install-snapshot rs-install-snapshot
   :request-vote rs-request-vote
   })


;;; Raft Server Implementation
;;;
;;; id - the ID of this server [static]
;;; log - a Log implementation [static]
;;; rpc - an RPC implementation that knows how to communicate will all servers
;;;       (by their ID) in the union of all the server-sets in the servers vector.
;;;       The RPC must implement the three-argument arity of all of the functions
;;;       defined in the RaftProtocol
;;; election-config - a map with the following entries [static]
;;;   :broadcast-timeout - time in ms for leader broadcast
;;;   :election-timeout-min - minimum time in ms for election timeout
;;;   :election-timeout-max - maximum time in ms for election timeout
;;; timers - a map (wrapped in a [ref]) whose keys are timer keywords and whose
;;;    values are functions (run in a future).  They all take a callback function
;;;    that is part of the Election protocol if they timeout.
;;;    keys include:
;;;      :follower  (follower-state election timeout)
;;;      :candidate (candidate-state timeout)
;;;      :broadcast (leader-state send-broadcast timeout)
;;; servers-config - a map with the following entries
;;;   :servers - a vector that contains one or two sets of server-ids for all
;;;             servers in the Raft cluster.  if there is only one entry in the
;;;             servers vector that is the current config
;;;             If there are two entries in the servers vector, the first
;;;             entry is the current config and the second entry is the new
;;;             config that we are transitioning to
;;;   :state-processors - a map whose keys are unique state-processor identifiers
;;;     and whose values are state-processor instances.
;;;   :state-change-listeners - a map whose keys are state-processor identifiers and
;;;     whose corresponding values are state-changed callback functions that
;;;     should be invoked when the state changes.
;;;   :leader - if not nil, using configured leader election protocol.  The value
;;;     should be the ID of ther server that is supposed to be the leader.
;;;   :max-entries (integer) - the maximum number of entries (commands) to include
;;;     in a single append-entries RPC.  If not specified the value of
;;;     MAX-ENTRIES-DEF will be used.
;;;   :down-follower-retry (integer, milliseconds) - The amount of time to wait, in
;;;     milliseconds before attempting to re-connect with a follower.  If not specified,
;;;     the value of DOWN-FOLLOWER-RETRY-DEF will be used.
;;; server-state - a map that is persisted automatically as values are changed. [ref]
;;;   :servers - initialized from servers-config.  After that the values here
;;;     override what is in servers-config
;;;   :leader - initialized from servers-config.  After that the value here
;;;     override what is in servers-config
;;;   :state - :follower, :candidate, :leader
;;;   :current-term - latest term this server has seen.  initialized to 0 on first boot
;;;   :voted-for - candidate that received vote in current term (null if none)
;;;   :commit-index - index of highest log entry known to be committed.  Initialized
;;;     to 0, increases monotonically
;;;   :last-applied - index of highest log entry that has been applied.
;;;   :last-applied-config - index of the highest :set-config log entry that has
;;;     been applied. If the server is running with a config that it received at
;;;     start-up and no changes to its :servers had been stored in its log, this
;;;     will be 0.  When a server starts, it should scan its log starting from
;;;     (inc (max :last-applied :last-applied-config)) to it's log's last-id-term.
;;;     If there are any :set-config commands stored in the log, it should apply
;;;     the most current :set-config found (if any) and update :last-applied-config
;;;     accordingly.  TODO: Implement this behavior
;;;   :last-leader-cmd-time - system time in ms when last cmd received from leader.
;;;      See spec, section 6, last paragraph.
;;; leader-state - a map that is persisted automatically as values are changed.  used only
;;;     if state = :leader [ref]
;;;   keys: <server-ids>
;;;   values: {
;;;     :next-index <int>
;;;     :match-index <int>
;;;     :updater <future>
;;;   }
;;;   next-index is the index of the next log entry to send to that server. It is
;;;     initialized to the leaders's max log id + 1.
;;;   match-index is the index of the highest log entry known to be
;;;     replicated to that server. It is initialized to 0 when a server becomes
;;;     leader.
;;;   updater - If as follower is behind, the leader will create an updater thread
;;;     that is responsible for bringing the follower up-to-date.

(defrecord RaftServer [id                ; server-id
                       log               ; Log instance
                       rpc               ; RPC instance
                       election-config   ; map
                       timers            ; map (ref)
                       servers-config    ; map (ref)
                       server-state      ; map (ref)
                       leader-state])    ; map (ref)


(extend RaftServer
  Election
  rs-basicraft-election
  RaftProtocol
  rs-basicraft-raftprotocol
  StateMachine
  rsm-state-machine
  StateChangedNotifier
  rsm-state-changed-notifier)

(actor-wrapper RaftServerActor [Election RaftProtocol] defrecord)

(defn make-simple-raft-server
  [id log rpc ec sc ss ls]
  (RaftServer. id log rpc ec (ref {}) sc
               (ref (assoc ss
                           :leader (:leader sc)
                           :servers (:servers sc)))
               (ref ls)))

(defn make-raft-server
  "Constructs a RaftServer instance that has been actor-fied.
   Parameters:
     id - the server id
     log - a Log instance
     rpc - an RPC instance (basically anything that implements the RaftProtocol
     election-config - a map with the following entries [static - NOT a ref]
       :broadcast-timeout - time in ms for leader broadcast
       :election-timeout-min - minimum time in ms for election timeout
       :election-timeout-max - maximum time in ms for election timeout
  "
  [id log rpc election-config servers-config server-state leader-state]
  (->RaftServerActor (chan) (make-simple-raft-server
                             id
                             log
                             rpc
                             election-config
                             servers-config
                             server-state
                             leader-state)))

(defn make-raft-from-config
  "Constructs a raft server from the provided configuration.
  Parameters:
  RAFT-CONFIG - Raft server configuration
  "
  [{:keys [:id :prefix :log :election-config :leader-state
           :rpc :servers-config :server-state]}]
  (logger/tracef (str "make-raft-from-config: id=%s, prefix=%s, log=%s, election-config=%s, "
                      "leader-state=%s, rpc=%s, servers-config=%s, server-state=%s")
                 id prefix log election-config leader-state rpc servers-config server-state)
  (let [dirs-map (util/init-directories prefix id :config :state :snapshots)
        log (apply (resolve (symbol (:fn log))) (cons dirs-map (:args log)))
        rpc (apply (resolve (symbol (:fn rpc))) (:args rpc))
        processors (into {} (map
                             (fn [[id {:keys [:fn :args]}]]
                               [id (apply (resolve (symbol fn)) (cons dirs-map (map eval args)))])
                             (:state-processors servers-config)))
        listeners (into {} (map
                            (fn [[id {:keys [:fn :args]}]]
                              [id (apply (resolve (symbol fn)) (map eval args))])
                            (:state-change-listeners servers-config)))]
    (RaftServer. id
                 log
                 rpc
                 election-config
                 (ref {})
                 (merge (assoc servers-config :state-processors processors :state-change-listeners listeners)
                        dirs-map)
                 (ref (assoc server-state :leader (:leader servers-config) :servers (:servers servers-config)))
                 (ref leader-state))))
