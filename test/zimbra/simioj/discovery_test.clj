(ns zimbra.simioj.discovery-test
  (:require [clojure.test :refer :all]
            [zimbra.simioj [actor :refer :all]]
            [zimbra.simioj.discovery :refer :all]
            [zimbra.simioj.config :as config]
            [zimbra.simioj.util :refer :all]))



(deftest update-topology-clustered-have-expected-nodes-test
  (testing "test have cluster with expected nodes"
    (let [now (timestamp-now)
          tm1 {:nodes {:n1 {:contacted now}
                      :n2 {:contacted now}
                       :n3 {:contacted now}}}
          tm1u (update-toplogy-clustered tm1 3 0 1000)]
      (is (:have-cluster? tm1u)))))


(deftest update-topology-clustered-have-quorum-with-timout-test
  (testing "test have cluster with quorum timeout"
    (let [now (timestamp-now)
          tm1 {:nodes {:n1 {:contacted now}
                      :n2 {:contacted now}
                       :n3 {:contacted 0}}
               :quorum-time (- now 1000)}
          tm1u (update-toplogy-clustered tm1 3 100 1000)]
      (is (:have-cluster? tm1u)))))


(deftest update-topology-clustered-have-quorum-no-timeout-test
  (testing "test no cluster before quorum timeout"
    (let [now (timestamp-now)
          tm1 {:nodes {:n1 {:contacted now}
                      :n2 {:contacted now}
                       :n3 {:contacted 0}}
               :quorum-time (- now 999)}
          tm1u (update-toplogy-clustered tm1 3 1000 1000)]
      (is (not (:have-cluster? tm1u))))))



(def ^:dynamic *nodes* (ref {}))

(defrecord TestDiscoveryRPC []
  DiscoveryRPC
  (notify-server! [this address node-map]
    (let [inst (@*nodes* address)]
      (if inst
        (notify! inst node-map)
        node-map))))

(defn- make-cfg [id name address initial-nodes expected-nodes]
  (let [cfg (-> config/default-config
                (assoc-in [:discovery :expected-nodes] expected-nodes)
                (assoc-in [:discovery :initial-nodes] initial-nodes)
                (assoc-in [:discovery :quorum-timeout] 1000)
                (assoc-in [:discovery :method] :unicast)
                (assoc-in [:node :data-dir] nil)  ; force non-persisting topology
                (assoc-in [:node :id] id)
                (assoc-in [:node :name] name)
                (assoc-in [:node :endpoint :http] address))]
    ;; (print "*****************\n")
    ;; (clojure.pprint/pprint cfg)
    ;; (print "\n********\n\n") (flush)
    cfg))

(def ^:dynamic *listener* (ref {}))

(defn- test-discovery-change-listener [old-topo new-topo]
  (dosync (alter *listener* assoc (:id new-topo) new-topo)))


(defn- instance
  "Each instance should be run in a separate thread."
  [rpc cfg listener]
  ;; give each thread it's own copy of state variables
  (printf "starting instance: %s\n" (.getId (Thread/currentThread))) (flush)
  (let [d (make-cluster-discovery rpc cfg listener)]
    (dosync (alter *nodes* assoc (get-in cfg [:node :endpoint :http]) d))
    (Thread/sleep 500)
    (discover d)
    d))


(deftest unicast-discovery-cluster-test
  (testing "unicast-discovery-test, 3 node cluster"
    (let [c1 (make-cfg :n1 "node1" "addr1" ["addr1"] 3)
          c2 (make-cfg :n2 "node2" "addr2" ["addr1"] 3)
          c3 (make-cfg :n3 "node3" "addr3" ["addr1"] 3)
          rpc (->TestDiscoveryRPC)
          cd1 (future (instance rpc c1 test-discovery-change-listener))
          cd2 (future (instance rpc c2 test-discovery-change-listener))
          cd3 (future (instance rpc c3 test-discovery-change-listener))]
      (print "sleeping for 10 secs\n") (flush)
      (Thread/sleep 10000)
      (doseq [d (list @cd1 @cd2 @cd3)]
        (stop-discovery d))
      (print "awakened from sleep\n") (flush)
      ;; (clojure.pprint/print-table (vals @*listener*)) (flush)
      ;; (clojure.pprint/pprint @*listener*) (flush)
      (is (= (count (keys @*listener*)) 3))
      (is (apply #'and (map :have-cluster? (vals @*listener*))))
      (is (apply #'and (map (comp pos? :quorum-time) (vals @*listener*)))))))
