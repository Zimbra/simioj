(ns zimbra.simioj.util
  (:gen-class)
  (:require [clojure.tools.reader.edn :as edn]
            [clojure.tools.logging :as logger]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as j]))

(defn persist-obj
  "Persist any object to disk.
  By default uses EDN format but accepts serializer function
  "
  [location obj & {:keys [serialize-func] :or {serialize-func identity}}]
  (spit location (serialize-func obj)))

(defn load-obj
  "Load any object persisted with `persist-obj`"
  [location & {:keys [deserialize-func] :or {deserialize-func edn/read-string}}]
  (deserialize-func (slurp location)))

(defn load-obj-if-present
  "Loads the object specified if it exists otherwise returns the default value
  The object is loaded using an alternate deserialization function if one is specified.
  "
  [location & {:keys [default-val deserialize-func] :or {default-val nil
                                                         deserialize-func edn/read-string}}]
  (if (.exists (io/file location))
    (load-obj location :deserialize-func deserialize-func)
    default-val))

(defn- update-or-insert!
  "Updates or inserts a new value in the table specified"
  [db-spec table [:key :value]]
  (j/with-db-transaction [c db-spec]
    (let [result (j/update! c table {:key key :value value} ["key = ?" key])]
      (if (zero? (first result))
        (j/insert! c table {:key key :value value})))))

(deftype SQLiteMap [db-spec table]
  clojure.lang.Associative
  (containsKey [this k]
    (some? (.valAt this k)))
  (entryAt [this k]
    (let [row (j/query db-spec [(str "SELECT value FROM " table " WHERE key=?") k])]
      (first row)))

  clojure.lang.IPersistentCollection
  (count [_]
    (let [result (j/query db-spec [(str "SELECT count(*) as count FROM " table)]
                          :row-fn :count)]
      (first result)))

  (cons [this [:key :value]]
    (do
      (update-or-insert! db-spec table [key value])
      this))

  (empty [this]
    (zero? (.count this)))

  (equiv [this o]
    (and
     (= (.count this) (count o))
     (every? identity
             (for [[:k :value] (seq this)]
               (= (k o) value)))))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    (do
      (update-or-insert! db-spec table [k v])
      this))
  (without [this k]
    (do
      (j/with-db-transaction [c db-spec]
        (j/delete! c table ["key=?" k]))
      this))

  clojure.lang.ILookup
  (valAt [this k not-found]
    (let [row (j/query db-spec [(str "SELECT value FROM " table " WHERE key=?") k])]
      (:value (first row))))
  (valAt [this k]
    (.valAt this k nil))

  clojure.lang.Seqable
  (seq [_]
    (let [s (seq (apply merge (j/query db-spec [(str "SELECT key, value FROM " table " ORDER BY key")]
                               :row-fn (fn [{:keys [:key :value]}]
                                         {(read-string key) value}))))]
;      (println "Total sequence: " s)
      s)))

(defn- initialize-db
  "Creates the tables for the log file"
  [db & {:keys [table] :or {table "cache"}}]
  (j/db-do-commands db
                    (j/create-table-ddl (str table)
                                        [:key "text" :primary :key]
                                        [:value "text"]
                                        :table-spec "without rowid")))

(defn make-sqlite-map
  "Returns an instance of a sqlite backed map"
  [location & {:keys [table] :or {table "cache"}}]
  (let [db-spec {:classname   "org.sqlite.JDBC"
                 :subprotocol "sqlite"
                 :subname location}]
    (when-not (.exists (io/file location))
      (initialize-db db-spec :table table))
    (->SQLiteMap db-spec table)))

(defn make-persisting-ref
  "Creates a ref that has a validator attached to it.  The validator's
   sole function is to persist the contents of the ref to a file whenever
   it is updated.
   Parameters:
     LOCATION - path to where the data is saved.
     :default <default-value> - if LOCATION exists, the ref is initialized
       with the data from that LOCATION.  It must be saved in EDN format.
       If LOCATION does not exist, the ref is populated with the :default
       value (nil if not provided)
   Returns:
     A ref
  "
  [location & {:keys [:default]}]
  (ref (if (.exists (java.io.File. location))
         (edn/read-string (slurp location))
         default)
       :validator
       (fn [data] (try
                    (spit location (pr-str data))
                    true
                    (catch Exception e (logger/errorf
                                        "Error saving to %s - %s"
                                        location (.getMessage e)))))))


(defmacro with-temp-file-path
  "Convience macro that will generate a unique termporary
   pathname in your system's default temp directory and then
   execute the code in BODY.
   Parameters:
     PVAR - the name of the symbol that will be bound to the
       temporary path.
     FEXT - the extension that will be taked on to the path; e.g.,
       \".edn\"
     BODY - The body that will be executed inside the let block
       that is created by this macro.
   This macro is useful in cases where you do not want the system
   to automatically create the file for you, as in the case of
   java.io.File/createTempFile"
  [pvar fext & body]
  `(let [~pvar (.getAbsolutePath
                 (clojure.java.io/file
                  (System/getProperty "java.io.tmpdir")
                  (str (java.util.UUID/randomUUID) ~fext)))]
     ~@body))

(defmacro with-cleanup-file-path
  "Execute BODY and delete file PVAR afterwards, if it exists.
   BODY is wrapped in a try block and the cleanup code is in a
   finally block.
   Parameters:
     PVAR - the name of the symbol that is expected to  be bound
       to the file path.
     BODY - the code to execute."
  [pvar & body]
  `(try
     ~@body
     (finally (let [f# (java.io.File. ~pvar)]
                (when (.exists f#) (.delete f#))))))
