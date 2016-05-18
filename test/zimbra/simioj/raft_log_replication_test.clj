(ns zimbra.simioj.raft-log-replication-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as logger]
            [zimbra.simioj.raft [server :refer :all]]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj.raft [rpc :refer :all]]))


(defn- make-log []
  (make-memory-log nil [{:id 1 :term 1 :rid 111 :command [:noop {}]}
                        {:id 2 :term 1 :rid 112 :command
                         [:patch {:oid 1
                                  :upsert {}
                                  :ops [(fn [v] (assoc v :name "widgetmaker"))
                                        (fn [v] (assoc v :city "houston"))
                                        (fn [v] (assoc v :employees 100))]}]}
                        {:id 3 :term 2 :rid 113 :command
                         [:patch {:oid 1
                                  :upsert {}
                                  :ops [(fn [v] (update v :employees inc))
                                        (fn [v] (assoc v :state "TX"))]}]}]))


(deftest leader-update-follower-test
  (testing "That a leader can bring a follower up to date"
    (let [sm (ref {})
          rpc (->TestRpc sm)
          sc {:servers [#{:s0 :s1 :s2}]}
          s0 (make-simple-raft-server
              :s0 (make-log) rpc {} ; id log rpc ec
              sc
              {:state :leader :current-term 2 :commit-index 3 :voted-for :s0} ; ss
              {:s1 {:next-index 4 :match-index 0}}) ; ls
          s1 (make-simple-raft-server
              :s1 (make-log) rpc {}
              sc
              {:state :follower :current-term 2 :commit-index 2 :voted-for :s0} ; ss
              {}) ; ls
          s2 (make-simple-raft-server
              :s2 (make-memory-log nil) rpc {}
              sc
              {:state :follower :current-term 1 :commit-index 0 :voted-for :s0} ; ss
              {}) ; ls
          ss0 (:server-state s0)
          ss1 (:server-state s1)
          ss2 (:server-state s2)
          _ (dosync (ref-set sm {:s0 s0 :s1 s1 :s2 s2}))
          r1 (command! s0 "r1" [:noop {}])]
      (is (= (:status r1) :accepted))
      (Thread/sleep 50)
      (is (= (:status (command! s0 "r2" nil)) :accepted))
      (is (apply = (map #(last-id-term (:log %)) [s0 s1 s2])))
      (is (apply = (map #(:current-term @(:server-state %)) [s0 s1 s2])))
      (is (apply = (map #(:commit-index @(:server-state %)) [s0 s1 s2]))))))


  "Returns an Entry that contains the maximum number of commands allowed
  based on the term of the log entry in FIRST-INDEX and the max-entries
  config value.
  Parameters:
  LOG - the log instance
  MAX-ENTRIES - the max number of enters to send in a single append-entries rpc call.
    All must belong to the same term.
  LEADER-ID - the id of the leader issuing the append-entries rpc
  LEADER-NEXT-INDEX - the leaders next-index value
  LEADER-TERM - the leader's current term
  LEADER-COMMIT-INDEX - the leaders current commit-index
  PREV-LOG-INDEX - the previous log index
  PREV-LOG-TERM - the previous log term
  FIRST-INDEX - the index of the first entry to send."


(deftest compute-entries-for-follower-test
  (testing "private function rs-compute-entries-for-follower"
    (let [ceff #'zimbra.simioj.raft.server/rs-compute-entries-for-follower
          log (make-log)
          entry (ceff log 5 :s0 4 2 3 3 2 1)
          entry2 (ceff log 5 :s0 4 2 3 3 2 3)]
      (is (= (count (:entries entry)) 2))
      (is (= (:id (first (:entries entry))) 1))
      (is (= (:id (second (:entries entry))) 2))
      (is (= (count (:entries entry2)) 1))
      (is (= (:id (first (:entries entry2))) 3)))))
