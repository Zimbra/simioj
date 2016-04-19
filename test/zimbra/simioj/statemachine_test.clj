(ns zimbra.simioj.statemachine-test
  (:require [clojure.test :refer :all]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj.raft [statemachine :refer :all]]))


(defn- make-log []
  (make-memory-log [{:id 1 :term 1 :rid 111 :command [:noop {}]}
                    {:id 2 :term 1 :rid 112 :command [:set-config {:servers [#{} #{:s0}]}]}
                    {:id 3 :term 1 :rid 113 :command [:patch {:oid 1
                                                              :upsert {}
                                                              :ops [#(assoc % :name "widgetmaker")
                                                                    #(assoc % :city "houston")
                                                                    #(assoc % :employees 100)]}]}]))


(deftest memory-statemachine-cmd-set-config
  (testing "that :set-config commands are processed immediately"
    (let [log (make-log)
          cb-state (ref '())
          cb-fn (fn [old-state new-state] (dosync (ref-set cb-state (list old-state new-state))))
          sm (make-memory-state-machine :set-config (ref {}) cb-fn)
          last-applied (process-log! sm log 0 0)]
      (is (= last-applied 3))
      (is (= @cb-state (list {} {:servers [#{} #{:s0}]})))
      (is (= (get-state sm) {:servers [#{} #{:s0}]})))))

(deftest memory-statemachine-cmd-noop
  (testing "that :noop commands honor commit-index"
    (let [log (make-log)
          cb-state (ref '())
          cb-fn (fn [old-state new-state] (dosync (ref-set cb-state (list old-state new-state))))
          sm (make-memory-state-machine :noop (ref {}) cb-fn)
          last-applied-0 (process-log! sm log 0 0)
          last-applied-1 (process-log! sm log 1 0)
          last-applied-2 (process-log! sm log 2 1)
          last-applied-3 (process-log! sm log 3 2)]
      (is (zero? last-applied-0))
      (is (= last-applied-1 1))
      (is (= last-applied-2 2))
      (is (= last-applied-3 3)))))

(deftest memory-statemachine-cmd-patch
  (testing "that :patch commands apply correctly"
    (let [log (make-log)
          cb-state (ref '())
          cb-fn (fn [old-state new-state] (dosync (ref-set cb-state (list old-state new-state))))
          sm (make-memory-state-machine :patch (ref {}) cb-fn)
          last-applied-0 (process-log! sm log 0 0)
          last-applied-3 (process-log! sm log 3 0)]
      (is (zero? last-applied-0))
      (is (= last-applied-3 3))
      (is (= (first @cb-state) {:oid 1 :val nil}))
      (is (= (second @cb-state) {:oid 1 :val {:name "widgetmaker" :city "houston" :employees 100}}))
      (is (= (post-cmd! log 1 114 [:patch {:oid 1
                                           :upsert {}
                                           :ops [#(update-in % [:employees] + 10)]}]) 4))
      (process-log! sm log 4 last-applied-3)
      (is (= (get-state sm 1) {:name "widgetmaker" :city "houston" :employees 110})))))
