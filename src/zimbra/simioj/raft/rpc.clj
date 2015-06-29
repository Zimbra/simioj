(ns zimbra.simioj.raft.rpc
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as logger]
            [zimbra.simioj.util :as util]
            [zimbra.simioj.raft.server :refer :all]
            [clojure.java.jdbc :as j])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; RPC - Test Implementation
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
