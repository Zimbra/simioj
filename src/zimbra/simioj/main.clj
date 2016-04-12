(ns zimbra.simioj.main
  (:require [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as logging]
            [zimbra.simioj.config :as config])
  (:gen-class))


(defn- ^:no-doc build-config [options]
  (let [config-base (if (:replace-config options)
                      (apply config/load-cfg-files! (clojure.string/split
                                                     (:replace-config options) #","))
                      config/default-config)
        config-final (if (:update-config options)
                       (config/load-config-maps!
                        config-base
                        (apply
                         config/load-cfg-files! (clojure.string/split
                                                 (:update-config options) #",")))
                       config-base)]
    config-final))


;; (defn- ^:no-doc -service-start [ctx]
;;   "Given the curent context CTX (which is wrapped in a ref),
;; start the registered services"
;;   (when (some identity (list [


(def cli-options
  [["-p" "--port PORT" "Port to listen for API."
    :default (last (clojure.string/split (get-in
                                          config/default-config
                                          [:node :endpoint :http]) #":"))
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-u" "--update-config FILE1,FILE2,..." "update default config"]
   ["-r" "--replace-config FILE1,FILE2,..." "replace default config"]
   ["-h" "--help" "Print this help."]])


(defn error-msg [errors]
  (str "Command line arguments are invalid:\n\n"
       (string/join \newline errors)))


(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn start
  "Handles the 'start' command"
  [options]
  (let [config (build-config options)
        rpc {} ; TODO
        raft {} ; TODO
        ctx (ref {:config config
                  :rpc rpc
                  :raft raft})]
    ;; TODO - start services
    (clojure.pprint/pprint @ctx)))


(defn usage [options-summary]
  (string/join
   \newline
   ["SIMIOJ - Distributed Task Execution Framework."
    ""
    "Usage: program-name [options] action"
    ""
    "Options:"
    options-summary
    ""
    "Default config loaded from"
    config/default-config-file
    ""
    "Actions:"
    "  start    Start the service"
    ""
    "Once started, the service will run until it is terminated."]))


(defn ^:no-doc -main
  "Provide support for running via command line with arguments.
  Run as follows:

      lein run
      lein run -- [ARGS]

  `lein run -- --help` will print full help message with all supported options.
  "
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))

    (case (first arguments)
      "start" (start options)
      (exit 1 (usage summary)))))
