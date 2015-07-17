(ns zimbra.simioj.actor
  (:require
   [clojure.core.async :refer [chan <!! >!! close!]]
   [clojure.tools.logging :as logger])
  (:gen-class))

(defprotocol Actor
  "Implement the Actor protocol when you need to be able to message an
  independent thread, synchronously or asynchronously.  Uses a channel
  that is supplied by a constructor function of the implementing record."
  (thread-cb [this]
    "This is the method that processes the channel message loop and
    drives the wrapped instance.")
  (start [this] "Start the actor.")
  (stop [this] "Stop the actor. NOTE: This will cause it channel to be closed.")
  (get-wrapped [this] "return the wrapped instance"))

(defmacro actor-wrapper
  "Simplify the process wrapping a deftype or defrecord that
  implements one or more protocols and running it in it's own
  execution thread.  This macro will generate a new deftype or defrecord TNAME
  that will implement the Actor protocol (see above) as well as
  providing wrapper functions for all protocols listed in the
  PROTOCALS list.   GENTYPE is the type of definition to generate.
  It should be deftype or defrecord.

  Example:

  Given two protocols FooProto and BarProto and an instance of a non-threaded
  deftype named FooBarLocal, bound to the symbol fbl, then:

  (actor-wrapper FooBarActor [Foo Bar] deftype)
  (def fba (->FooBarActor (chan) fbl))
  (start fba)  ;; initializes the thread that runs the FooBarLocal instance fbl
  (foo-method fba args)
  ...
  (stop fba)   ;; terminates the thread that runs the FooBarLocal instance fbl
  "
  [tname protocols gentype]
  (let [this# (gensym 'this)
        argbodies# (fn [fname# alists#]
                     (map (fn [al#]
                            (let [al2# (cons this# (rest al#))]
                              (list
                               fname#
                               (vec al2#)
                               `(let [c# (chan)]
                                  (>!! ~'channel (list
                                                  (fn [~this#]
                                                    ~(cons fname# al2#))
                                                  c#))
                                  (<!! c#)))))
                          alists#))
        functions# (mapcat (fn [p#]
                             (let [pp# (if (symbol? p#) (eval p#) p#)]
                               (cons p#
                                     (mapcat #(argbodies# (:name %) (:arglists %))
                                             (vals (:sigs pp#))))))
                           protocols)]
    `(~gentype ~tname [~'channel ~'wrapped]
       Actor
       (thread-cb [this#]
         (loop [[cmd# rchan#] (<!! ~'channel)]
           (when cmd#
             (try
               (let [resp# (cmd# ~'wrapped)]
                 (when-not (nil? resp#)
                   (>!! rchan# resp#)))
               (catch Exception e# (do (logger/error e# "error processing command")))
               (finally (close! rchan#)))
             (recur (<!! ~'channel)))))
       (start [this#]
         (future (thread-cb this#)))
       (stop [this#]
         (>!! ~'channel [nil nil]))
       (get-wrapped [this#]
         ~'wrapped)
       ~@functions#)))


(defn actor-message-loop
  "This function will spin up a message processing loop in a thread.
  It returns a channel that can be used to send commands to the
  message-loop thread.  The commands that are sent on the channel
  should look like this: [cmd c], where:
  cmd is an fn or function that takes one argument (that is the actor-state
  that was passed into his function, initially) and c is a channel used
  to sent the response back on.   This channel c will be closed after the
  reponse is sent.  The cmd fn must return the following:
  [response new-state].  The response is what is sent back on the response
  channel and new-state will be the state value passed in as an argument to the
  next function that is sent.
  If an exception is raised during the execution fo the cmd function, the following
  will be sent back over the response channel {:error <error message>}"
  [actor-state]
  (let [input-channel (chan)
        loopfn (fn [channel state]
                 (loop [[cmd rchan] (<!! channel)
                        state state]
                   (if cmd
                     (let [[resp newstate]
                           (try
                             (cmd state)
                             (catch Exception e (do
                                                  (logger/error e "error processing command")
                                                  (list {:error (.getMessage e)} state))))]
                       (>!! rchan resp)
                       (close! rchan)
                       (recur (<!! channel) newstate))
                     (close! channel))))]
    (future (loopfn input-channel actor-state))
    input-channel))


(defprotocol MessageLoop
  "Simple protocol for working with a channel that is returned
  from actor-message-loop."
  (send-cmd [this cmd]
    "Send a command CMD to the message loop.  Commands MUST
     be a function that takes one argument (state) and returns
     [response new-state].")
  (send-stop [this]
    "Terminate the thread that is started by actor-message-loop."))


(deftype ActorLoop [channel]
  MessageLoop
  (send-cmd [this cmd]
    (let [c (chan)]
      (>!! channel [cmd c])
      (<!! c)))
  (send-stop [this]
    (>!! channel [nil nil])))

(defn make-message-loop
  "Returns and instance of a MessageLoop that is ready to process commands."
  [initial-state]
  (->ActorLoop (actor-message-loop initial-state)))
