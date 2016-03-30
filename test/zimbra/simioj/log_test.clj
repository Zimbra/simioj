(ns zimbra.simioj.log-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as logger]
            [zimbra.simioj.raft [log :refer :all]]
            [zimbra.simioj [util :refer :all]]))


(defn- log-basic-test [l]
  (is (= (first-id-term l) [0 0]))
  (is (= (last-id-term l) [0 0]))
  (is (nil? (get-entry l 1)))
  (is (= (post-cmd! l 1 "r1" [:noop {}]) 1))
  (is (thrown? IllegalArgumentException (post-cmd! l 1 "r1" [:noop {}])))
  (is (put-cmd! l 2 1 "r2" [:noop {}]))
  (is (not (put-cmd! l 4 1 "r3" [:noop {}])))
  (is (= (last-id-term l) [2 1]))
  (is (= (get-entry l 2) {:id 2 :term 1 :rid "r2" :command [:noop {}]}))
  (is (= (first-id-term l) [1 1]))
  (is (= (post-cmd! l 1 "r3" [:noop {}]) 3))
  (is (= (post-cmd! l 2 "r4" [:noop {}]) 4))
  (is (= (first-id-term l) [1 1]))
  (is (= (last-id-term l) [4 2]))
  (is (ltrim-log! l 1))
  (is (= (first-id-term l) [2 1]))
  (is (rtrim-log! l 4))
  (is (= (first-id-term l) [2 1]))
  (is (= (last-id-term l) [3 1]))
  (is (not (ltrim-log! l 1)))
  (is (not (rtrim-log! l 4)))
  (is (= (first-id-term l) [2 1]))
  (is (= (last-id-term l) [3 1])))

(deftest memory-log-basic-test
  (testing "memory-log: basic tests"
    (log-basic-test (make-memory-log))))

(deftest sqlite-log-basic-test
  (testing "sqlite-log: basic tests"
    (with-temp-file-path log-file ".dat"
      (with-cleanup-file-path log-file
        (log-basic-test (make-sqlite-log log-file))))))
