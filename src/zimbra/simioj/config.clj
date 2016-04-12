(ns zimbra.simioj.config
 (:gen-class)
  (:require [clojure.tools.reader.edn :as edn]
            [clojure.java.io :as io]))

(def default-config-file
  (io/resource "defaults.edn"))


(def default-config
  (->> default-config-file
       slurp
       edn/read-string))

;; -config is static information that is unique for each
;; node in the cluster
(defonce ^:dynamic -config (ref default-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Configuration loading and merging
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:no-doc -deep-merge
  "Deep merge two maps"
  [& values]
  (if (every? map? values)
    (apply merge-with -deep-merge values)
    (last values)))

(defn ^:no-doc -update-config
  "Replaces the value of the config with the provided value"
  [config value]
  (dosync
   (ref-set config value)))

(defn ^:no-doc -load-from
  "This is intended to load the configuration data from a file(s)"
  [files]
  (println (str "Loading configuration from: " files))
  )

(defn override-config!
  "Override the default config information with the values from
   the supplied map CFG.
   This is useful for testing to ensure a specified starting config"
  [cfg]
  (dosync
   (ref-set -config cfg)))

(defn load-cfg-file
  "Loads the configuration from the specified FILE if it exists. Returns
  the loaded configuration map or an empty map if the FILE does not exist."
  [file]
  (if (.exists (io/as-file file))
    (edn/read-string (slurp file))
    {}))

(defn load-config-edns!
  "Load config from the specified EDN strings.  Returns the
  system state."
  [& edns]
  (-update-config -config (reduce -deep-merge (map edn/read-string
                                                   edns)))
  @-config)


(defn load-config-maps!
  "Load config from the specified maps.  Returns the system state."
  [& maps]
  (-update-config -config (reduce -deep-merge maps))
  @-config)


(defn load-cfg-files!
  "Loads all of the local configuration from the list of FILES,
  updates the -config ref, and returns the system state."
  [& files]
  (let [lcfg (reduce -deep-merge (map load-cfg-file files))]
    (-update-config -config lcfg)
    @-config))

(defn config
  "Retrieve the current configuration, optionally overriding
   it with information from the options CFGS seq."
  [& cfgs]
  (reduce -deep-merge (cons @-config cfgs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Config helper functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn local-node-info
  "Given a map containing {:node <node-configuration>}
   return a map of the following format that is used typically during discovery.
   {<local-node-id> {:name <local-node-name> :address <local-node-address>}}"
  [cfg]
  (let [node (:node cfg)
        id (:id node)
        name (:name node)
        address (:http (:endpoint node))]
    {id {:name name :address address}}))

(defn node-info-address
  "Given NODE-INFO (like what is returned from the local-node-info function,
  return the address of it's cluster endpoint in the form \"<ip-address>:port\""
  [node-info]
  (:address (first (vals node-info))))
