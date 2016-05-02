(ns zimbra.simioj.raft-command-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as logger]
            [zimbra.simioj.raft [server :refer :all]]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj.raft [rpc :refer :all]]))


(deftest single-server-test
  (testing "Basic commands sent to single server"
    (let [log (make-memory-log nil)
          s0 (make-simple-raft-server
              :s0 log nil {} ; id log rpc ec
              {:servers [#{:s0}]} ; sc
              {:state :leader :current-term 1 :commit-index 0} ; ss
              {}) ; ls
          sc (:servers-config s0)
          ss (:server-state s0)
          ls (:leader-state s0)]
      (is (= (command! s0 "r1" [:noop {}]) {:status :accepted :server :s0}))
      (is (= (command! s0 "r2" [:noop {}]) {:status :accepted :server :s0}))
      (is (= (first-id-term log) [1 1]))
      (is (= (last-id-term log) [2 1]))
      (is (= (:commit-index @ss) 2)))))


(deftest multi-server-test-good
  (testing "Basic commands sent to leader/follower"
    (let [sm (ref {})
          rpc (->TestRpc sm)
          s0 (make-simple-raft-server
              :s0 (make-memory-log nil) rpc {} ; id log rpc ec
              {:servers [#{:s0 :s1}]} ; sc
              {:state :leader :current-term 1 :commit-index 0 :voted-for :s0} ; ss
              {}) ; ls
          s1 (make-simple-raft-server
              :s1 (make-memory-log nil) rpc {}
              {:servers [#{:s0 :s1}]} ; sc
              {:state :follower :current-term 1 :commit-index 0 :voted-for :s0} ; ss
              {}) ; ls
          ss0 (:server-state s0)
          ss1 (:server-state s1)
          ]
      (dosync (ref-set sm {:s0 s0 :s1 s1}))
      (is (= (command! s0 "r1" [:noop {}]) {:status :accepted :server :s0}))
      (is (= (command! s0 "r2" [:noop {}]) {:status :accepted :server :s0}))
      (is (= (:commit-index @ss0) 2))
      (is (= (:commit-index @ss1) 1)))))


(deftest multi-server-test-bad
  (testing "Basic command sent to leader that is behind follower"
    (let [sm (ref {})
          rpc (->TestRpc sm)
          s0 (make-simple-raft-server
              :s0 (make-memory-log nil) rpc {} ; id log rpc ec
              {:servers [#{:s0 :s1}]} ; sc
              {:state :leader :current-term 1 :commit-index 0 :voted-for :s0} ; ss
              {}) ; ls
          s1 (make-simple-raft-server
              :s1 (make-memory-log nil) rpc {}
              {:servers [#{:s0 :s1}]} ; sc
              {:state :follower :current-term 2 :commit-index 0 :voted-for :s0} ; ss
              {}) ; ls
          ss0 (:server-state s0)
          ss1 (:server-state s1)
          ]
      (dosync (ref-set sm {:s0 s0 :s1 s1}))
      (is (= (command! s0 "r1" [:noop {}]) {:status :moved :server :s1})))))


;; (require '[clojure.tools.namespace.repl :refer [refresh]])
;; (refresh)
