(ns zimbra.simioj.raft.log
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as logger]
            [zimbra.simioj.util :as util]
            [clojure.java.jdbc :as j])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Log Protocol
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol Log
  "The Raft Log protocol contains the functions that must be implemented to support
  an append-only Raft log.  See the MemoryLog for an example implementation."
  (first-id-term [this]
    "Returns:
       A vector of [<first-log-entry-index> <first-log-entry-term>]
       If the log is empty, returns [0 0]")
  (last-id-term [this]
    "Returns:
       A vector of [<last-log-entry-index> <last-log-entry-term>]")
  (post-cmd! [this term rid command]
    "Stores one new command in the log at the next log index number.
    NOTE: This is only ever used by the leader!
    Parameters:
      THIS - the Log instance
      TERM - the leader term number (int)
      RID - the request ID (str, normally a UUID - is unique!)
      COMMAND - a command to apply to the state machine. This is a vector
        of the form [<command-keyword> <command-map>]
    Returns:
      The log index number where the entry was stored.
    Raises:
      IllegalArgumentException - If another entry with the
        same RID exists in the log.
    ")
  (put-cmd! [this id term rid command]
    "Store one command in the log.
     NOTE: This is only ever used by followers!
    Parameters:
      THIS - the Log instance
      ID - the log index number
      TERM - the leader term number
      RID - the request ID (normally a UUID - is unique!)
      COMMAND - a command to apply to the state machine
    Returns:
      true if the last entry in the log prior to this entry
        existed.  This indicates that the put command
        succeeded.
      false if the last entry in the log prior to this entry
        did not exist.  in this case the command failed and
        the new entry was not stored in the log.")
  (get-entry [this id]
    "Retrieves the log entry located at log index ID.
    Returns:
      nil if log entry at index number ID does not exist.
      {:id <id> :term <term> :rid <rid> :command <command>} otherwise")
  (ltrim-log! [this last-id]
    "Remove all leading log entries up-to-and-including the entry
     with id LAST-ID.  Any entries with higher ID number will
     not be affected.
     Returns: true if an entry with id = LAST-ID existed, else
              false")
  (rtrim-log! [this first-id]
    "Remove all log entries starting with FIRST-ID.
     Any entries with lower a ID number will not be affected.
     Returns: true if an entry with id = FIRST-ID existed, else
              false"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Raft MemoryLog - Test Implementation (memory only, no persistence)
;;;; Not for use in production.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; log must be a vector wrapped in a ref
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
      (logger/tracef "post-cmd!: term=%d, rid=%s, lid=%d, lterm=%d, nid=%d, nidx=%d"
                     term rid lid lterm nid nidx)
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
      (logger/tracef "put-cmd!: id=%d, term=%d, rid=%s, lid=%d, lterm=%d, nidx=%s"
                     id term rid lid lterm nidx)
      (if (= lid (dec id))
        (dosync
         (alter log #(assoc % nidx entry))
         true)
        false)))
  (get-entry [this id]
    (let [[fid fterm] (first-id-term this)
          i (- id fid)
          clog (count @log)]
      (logger/tracef "get-entry: id=%d, fid=%d, fterm=%d, i=%s, clog=%d"
                     id fid fterm i clog)
      (when (and (not (neg? i)) (< i clog))
        (nth @log i))))
  (ltrim-log! [this last-id]
    (let [[fid fterm] (first-id-term this)
          ilast (- last-id fid)]
      (logger/tracef "ltrim-log!: last-id=%d, fid=%d, fterm=%d, ilast=%d"
                     last-id fid fterm ilast)
      (if (and (>= ilast 0)
               (< ilast (count @log)))
        (dosync
         (alter log #(subvec % (inc ilast)))
         true)
        false)))
  (rtrim-log! [this first-id]
    (let [[fid fterm] (first-id-term this)
          ilast+1 (- first-id fid)]
      (logger/tracef "rtrim-log!: first-id=%d, fid=%d, fterm=%d, ilast+1=%d"
                     first-id fid fterm ilast+1)
      (if (and (>= ilast+1 0)
               (< ilast+1 (count @log)))
        (dosync
         (alter log #(subvec % 0 ilast+1))
         true)
        false))))

(defn make-memory-log
  "Create a MemoryLog instance.  This is for testing purposes."
  ([] (make-memory-log []))
  ([log] (->MemoryLog (ref log))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Raft PersistentMemoryLog - Test Implementation (memory only,
;;;; but will persist)
;;;; Non-performant. Not for use in production.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- persist-if-changed [location vref expr]
  (let [ref-val (deref vref)
        rvalue (expr)]
    (when-not (= ref-val (deref vref))
      (logger/tracef "Value was changed.. Persisting %s to %s" @vref location)
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



(defn make-persistent-log
  "Create a PersistentMemoryLog instance."
  ([location & {:keys [data] :or {data []}}]
   (->PersistentMemoryLog location (make-memory-log (util/load-obj-if-present
                                                     location :default-val data)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; SQLiteLog Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype SQLiteLog [db table]
  Log
  (first-id-term [this]
    (let [row (j/query db [(str "SELECT min(id) as min_id, min(term) as min_term from " table)])
          result (first row)]
      (if (nil? (:min_term result))
        [0 0]
        [(:min_id result) (:min_term result)])))
  (last-id-term [this]
    (let [row (j/query db [(str "SELECT max(id) as max_id, max(term) as max_term from " table)])
          result (first row)]
      (if (nil? (:max_term result))
        [0 0]
        [(:max_id result) (:max_term result)])))
  (post-cmd! [this term rid command]
    (let [[lid, lterm] (last-id-term this)
          nid (inc lid)]
      (logger/tracef "post-cmd!: term=%d, rid=%s, lid=%d, lterm=%d, nid=%d"
                     term rid lid lterm nid)
      (try
        (j/with-db-transaction [c db]
          (j/insert! c table {:id nid :term term :rid rid :command (pr-str command)})
          nid)
        (catch Exception e (throw (IllegalArgumentException.
                                   (format "another entry with request id %s already in log" rid)))))))
  (put-cmd! [this id term rid command]
    (let [[lid lterm] (last-id-term this)
          nidx (inc lid)]
      (logger/tracef "put-cmd!: id=%d, term=%d, rid=%s, lid=%d, lterm=%d, nidx=%s"
                     id term rid lid lterm nidx)
      (if (= lid (dec id))
        (do
          (try
            (j/with-db-transaction [c db]
              (let [result (j/update! c table {:term term :rid rid :command (pr-str command)} ["id = ?" id])]
                (if (zero? (first result))
                  (j/insert! c table {:id nidx :term term :rid rid :command (pr-str command)}))))
            (catch Exception e (throw (IllegalArgumentException.
                                       (format "another entry with request id %s already in log" rid)))))
          true)
        false)))
  (get-entry [this id]
    (let [row (j/query db [(str "SELECT * from " table " WHERE id=?") id])
          element (first row)]
      (logger/tracef "get-entry: id=%d" id)
      (if element
        (update-in element [:command] read-string))))
  (ltrim-log! [this last-id]
    (let [entry (get-entry this last-id)]
      (logger/tracef "ltrim-log!: last-id=%d, fid=%s, fterm=%s"
                     last-id (:id entry) (:term entry) )
      (if (= last-id (:id entry))
        (do
          (j/with-db-transaction [c db]
            (j/delete! c table ["id <= ?" last-id]))
          true)
        false)))
  (rtrim-log! [this first-id]
    (let [entry (get-entry this first-id)]
      (logger/tracef "rtrim-log!: first-id=%d, fid=%s, fterm=%s"
                     first-id (:id entry) (:term entry))
      (if-not (nil? entry)
        (do
          (j/with-db-transaction [c db]
            (j/delete! c table ["id >= ?" first-id]))
          true)
        false))))


(defn- initialize-db
  "Creates the tables for the log file"
  [db & {:keys [table initial_data] :or {table "log"
                                         initial_data []}}]
  (do
    (j/db-do-commands db
                      (j/create-table-ddl (str table)
                                          [:id "integer" :primary :key]
                                          [:term "integer" :not :null]
                                          [:rid "text" :unique :not :null]
                                          [:command "text"]
                                          :table-spec "without rowid"))
    (when (seq initial_data)
      (j/with-db-transaction [c db]
        (doall (map #(j/insert! db table %) initial_data))))))

(defn make-sqlite-log
  "Creates or loads an existing log"
  [location & {:keys [table initial_data] :or {table "log" initial_data []}}]
  (let [db-spec {:classname   "org.sqlite.JDBC"
                 :subprotocol "sqlite"
                 :subname location}]
    (when-not (.exists (io/file location))
      (initialize-db db-spec :table table :initial_data initial_data))
    (->SQLiteLog db-spec table)))
