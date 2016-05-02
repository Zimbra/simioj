(ns zimbra.simioj.statemachine-test
  (:require [clojure.test :refer :all]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj.raft [statemachine :refer :all]]
            [zimbra.simioj.raft [server :refer :all]]))



(defrecord TestMachine [servers-config server-state])


(extend TestMachine
  StateMachine
  rsm-state-machine
  StateChangedNotifier
  rsm-state-changed-notifier)


(defn- make-state-machine [listeners]
  (TestMachine.
   {:state-processors {:kv (make-memory-state-processor {} :patch (ref {}))}
    :state-change-listeners listeners}
   (ref {:servers [#{:s0}]})))


(deftest set-config-cmd-test
  (testing ":set-config command")
  (let [cb-state (ref '())
        cb-fn (fn [cmd ostate nstate] (dosync (ref-set cb-state (list cmd ostate nstate))))
        sm (make-state-machine {:config [cb-fn]})]
    (process-set-config! sm [:set-config [#{:s1}]])
    (is (= (first @cb-state) :set-config))
    (is (= (second @cb-state) [#{:s0}]))
    (is (= (nth @cb-state 2) [#{:s1}]))))


(deftest patch-cmd-test
  (testing ":patch command")
  (let [cb-state (ref '())
        cb-fn (fn [cmd ostate nstate] (dosync (ref-set cb-state (list cmd ostate nstate))))
        sm (make-state-machine {:kv [cb-fn]})]
    (process-command! sm [:patch {:oid 1
                                  :upsert {}
                                  :ops [#(assoc % :name "widgetmaker")
                                        #(assoc % :city "houston")
                                        #(assoc % :employees 100)]}])
    (is (= (first @cb-state) :patch))
    (is (= (second @cb-state) {:oid 1 :val nil}))
    (is (= (nth @cb-state 2) {:oid 1 :val {:name "widgetmaker" :city "houston" :employees 100}}))
    (process-command! sm [:patch {:oid 1
                                  :upsert {}
                                  :ops [#(update % :employees inc)]}])
    (is (= (nth @cb-state 2) {:oid 1 :val {:name "widgetmaker" :city "houston" :employees 101}}))))
