(ns zimbra.simioj.raft-election-test
  (:require [clojure.test :refer :all]
            [clojure.core [async :refer [chan]]]
            [clojure.tools.logging :as logger]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.raft [server :refer :all]]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj.raft [rpc :refer :all]]))



(defn- register-server
  "Register a SERVER with ID to the pool that is managed by the
  RAFT-RPC"
  [raft-servers id server]
  (dosync (alter raft-servers assoc id server))
  server)

(defn- make-server [raft-servers id log rpc ec sc ss ls]
  (register-server raft-servers id (make-simple-raft-server id log rpc ec sc ss ls)))

(deftest candidate-test
  (testing "candidate! with basic 3 server raft cluster"
    (let [raft-servers (ref {})
          raft-rpc (->TestRpc raft-servers)
          sc {:servers [#{:s0 :s1 :s2}]}
          ec {:broadcast-timeout 10 :election-timeout-min 150 :election-timeout-max 300}
          s0 (make-server raft-servers :s0 (make-memory-log) raft-rpc ec sc
                          {:state :leader :current-term 1 :voted-for :s0
                           :commit-index 0 :last-applied 0}
                          {:s1 {:next-index 1 :match-index 0}
                           :s2 {:next-index 1 :match-index 0}})
          s1 (make-server raft-servers :s1 (make-memory-log) raft-rpc ec sc
                          {:state :follower :current-term 1 :voted-for :s0
                           :commit-index 0 :last-applied 0}
                          {})
          s2 (make-server raft-servers  :s2 (make-memory-log) raft-rpc ec sc
                          {:state :follower :current-term 1 :voted-for :s0
                           :commit-index 0 :last-applied 0}
                          {})]
      (dorun (map start-timers! [s0 s1 s2]))
      ;; first, establish s0 as :leader by sending out :noop
      (println (command! s0 "r1" [:noop {}])) (flush)
      (is (= (last-id-term (:log s0)) [1 1]))
      (is (= (last-id-term (:log s1)) [1 1]))
      (is (= (last-id-term (:log s2)) [1 1]))
      (Thread/sleep 10000))))



(deftest three-server-election-test
  (testing "test pure election with three servers"
    (let [raft-servers (ref {})
          raft-rpc (->TestRpc raft-servers)
          sc {:servers [#{:s0 :s1 :s2}]}
          ec {:broadcast-timeout 10 :election-timeout-min 150 :election-timeout-max 300}
          ecf {:broadcast-timeout 10 :election-timeout-min 300 :election-timeout-max 600}
          s0 (make-server raft-servers :s0 (make-memory-log) raft-rpc ec sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})
          s1 (make-server raft-servers :s1 (make-memory-log) raft-rpc ec sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})
          s2 (make-server raft-servers :s2 (make-memory-log) raft-rpc ec sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})]
      (dorun (map follower! [s0 s1 s2]))
      (Thread/sleep 2000)
      ;; (println "----- Server States -----")
      ;; (doseq [s [s0 s1 s2]]
      ;;   (printf "Server=%s, State=%s\n"
      ;;           (:id s) @(:server-state s))
      ;;   (flush))
      (is (= (count (filter #(= :leader (:state @(:server-state %))) [s0 s1 s2])) 1))
      (is (= (count (filter #(= :follower (:state @(:server-state %))) [s0 s1 s2])) 2))
      (let [leader (first (filter #(= :leader (:state @(:server-state %))) [s0 s1 s2]))
            followers (filter #(= :follower (:state @(:server-state %))) [s0 s1 s2])]
        (is (some? leader))
        (when leader
          (is (= (command! leader "r1" [:noop {}]) {:status :accepted :server (:id leader)}))
          (Thread/sleep 20)
          (doseq [f followers]
            (is (= (command! f (generate-rid) [:noop {}]) {:status :moved :server (:id leader)}))))))))


(deftest three-server-configured-election-test
  (testing "test configured election"
    ;; start :s1 and :s2 with larger election timeouts to guarantee that :s0
    ;; wins initial election
    (let [raft-servers (ref {})
          raft-rpc (->TestRpc raft-servers)
          sc {:servers [#{:s0 :s1 :s2}]}
          ec {:broadcast-timeout 100 :election-timeout-min 500 :election-timeout-max 1000}
          ecf {:broadcast-timeout 100 :election-timeout-min 1000 :election-timeout-max 1500}
          s0 (make-server raft-servers :s0 (make-memory-log) raft-rpc ec sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})
          s1 (make-server raft-servers :s1 (make-memory-log) raft-rpc ecf sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})
          s2 (make-server raft-servers :s2 (make-memory-log) raft-rpc ecf sc
                          {:current-term 0
                           :commit-index 0 :last-applied 0}
                          {})]
      (dorun (map follower! [s0 s1 s2]))
      (Thread/sleep 1000)
      ;; (println "----- Server States 1 -----")
      ;; (doseq [s [s0 s1 s2]]
      ;;   (printf "Server=%s, State=%s\n"
      ;;           (:id s) @(:server-state s))
      ;;   (flush))
      ;; Ensure :s0 won leadership initially
      (is (= :leader (:state @(:server-state s0))))
      (is (= :follower (:state @(:server-state s1))))
      (is (= :follower (:state @(:server-state s2))))
      ;; update servers-config to turn on configured election
      (dorun (map #(dosync (ref-set (:servers-config %) (assoc sc :leader :s2))) [s0 s1 s2]))
      (Thread/sleep 3000)
      ;; (println "----- Server States 2 -----")
      ;; (doseq [s [s0 s1 s2]]
      ;;   (printf "Server=%s, State=%s\n"
      ;;           (:id s) @(:server-state s))
      ;;   (flush))
      ;; ensure :s2 reclaimed leadership
      (is (= :follower (:state @(:server-state s0))))
      (is (= :follower (:state @(:server-state s1))))
      (is (= :leader (:state @(:server-state s2)))))))
