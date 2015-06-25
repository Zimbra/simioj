(ns zimbra.simioj.endpoint.http
  (:require
   [clojure.core.async :refer [chan <!! >!! close!]]
   [clojure.tools.logging :as logger])
  (:gen-class))
