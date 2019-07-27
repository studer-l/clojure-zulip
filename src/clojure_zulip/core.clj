(ns clojure-zulip.core
  (:require [clojure.core.async :as async]
            [cheshire.core :as cheshire]
            [clojure.tools.logging :refer [info debug trace]]
            [clojure-zulip.client :as client]))

;; connection management

(def default-connection-opts
  {:username nil
   :api-key nil
   :base-url "https://api.zulip.com/v1/"})

(defn connection
  "Create a new connection pool based on the given user-opts. Return a dict of
  the connection elements."
  [user-opts]
  (let [opts (merge default-connection-opts user-opts)]
    {:opts opts}))

(defn close!
  "Attempt to kill any request threads still running."
  ([] (shutdown-agents))
  ([conn] (close!)))

;; basic API commands, typically one per endpoint

(defn send-private-message
  "Send a private message to specified users. Returns a channel with
  the result."
  [conn users message]
  (trace "send-private-message" message "to" users)
  (client/request :POST conn "messages"
                  {:type "private"
                   :content message
                   :to (cheshire/generate-string users)}))

(defn send-stream-message
  "Send a message to a specific stream. Returns a channel with the
  result."
  [conn stream subject message]
  (trace "send-stream-message" message "in" stream "to" subject)
  (client/request :POST conn "messages"
                  {:type "stream"
                   :content message
                   :subject subject
                   :to stream}))

(defn send-message
  "Dispatches to relevant send message function based on whether a
  stream (string) or list of users is given."
  [& more]
  (if (string? (second more))
    (apply send-stream-message more)
    (apply send-private-message more)))

(defn update-message
  "Update content of message"
  [conn msg-id content]
  (trace "updating" msg-id "to" content)
  (client/request :PATCH conn (str "messages/" msg-id)
                  {:message_id msg-id
                   :content content}))

(defn register
  "Register a queue to listen for certain event types. Returns a
  channel with :queue_id, :max_message_id, and :last_event_id keys."
  ([conn] (register conn ["message" "subscriptions" "realm_user" "pointer"]))
  ([conn event-types] (register conn event-types false))
  ([conn event-types apply-markdown]
   (trace "Requesting new event queue")
   (client/request :POST conn "register"
                   {:event_types (cheshire/generate-string event-types)
                    :apply_markdown apply-markdown})))

(defn events
  "Get events from the specified queue occuring after last-event-id. Returns a
  channel."
  ([conn queue-id last-event-id] (events conn queue-id last-event-id false))
  ([conn queue-id last-event-id dont-block]
   (client/request :GET conn "events"
                   {:queue_id queue-id
                    :last_event_id last-event-id
                    :dont_block dont-block})))

(defn members
  "Get members of your entire organization. Returns a channel.
   TODO: Can this return for only one or more streams?"
  [conn]
  (client/request :GET conn "users"))

(defn subscriptions
  "Get a list of current subscriptions. Returns a channel."
  [conn]
  (client/request :GET conn "users/me/subscriptions" {}))

(defn add-subscriptions
  "Subscribe to the specified streams. Returns a channel."
  [conn streams]
  (let [stream-join-dicts (map #(hash-map "name" %) streams)]
    (client/request :POST conn "users/me/subscriptions"
                    {:subscriptions (cheshire/generate-string stream-join-dicts)})))

(defn remove-subscriptions
  "Unsubscribe from the specified streams. Returns a channel."
  [conn streams]
  (client/request :PATCH conn "users/me/subscriptions"
                  {:delete (cheshire/generate-string streams)}))

(defn- retry-failure?
  "Returns truthy if this is a kind of connection failure that can be solved by
  retrying after some delay."
  [r]
  (->> r
       ex-data
       :type
       (contains? #{:zulip-bad-request :zulip-unauthorized})
       not))

(defn- exception? [ex] (instance? Exception ex))

(defn cancelable-retry
  "In go-loop awaits `async-fn` for non-exception result. If anything is put on
  kill-channel, returns `:killed"
  [kill-channel async-fn]
  (let [publish-channel (async/chan)]
    (async/go-loop [attempt 0]
      (let [retry-channel (async-fn)
            [res ch]      (async/alts! [kill-channel retry-channel]
                                       :priority true)]
        (cond
          (= ch kill-channel)  (do (trace "retrying received kill signal")
                                   (async/>! publish-channel :killed))
          (retry-failure? res) (do (trace "Re-trying failed, waiting")
                                   (async/<! (async/timeout 5000))
                                   (trace "Re-trying, attempt" attempt)
                                   (recur (inc attempt)))
          (exception? res)     (do (trace "Exception during retrying:" res)
                                   (async/>! publish-channel :killed))
          ;; success
          :else                (async/>! publish-channel res))))
    publish-channel))

;; higher-level convenience functions built on top of basic API commands

(defn- subscribe-events*
  "Launch a goroutine that continuously publishes events on the
  publish-channel until an exception is encountered, the connection
  is closed or the kill-channel receives an event."
  [conn queue-id last-event-id publish-channel kill-channel reconnect?]
  (async/go-loop [queue-id queue-id
                  last-event-id last-event-id]
    (trace "Waiting for new events, last-event-id =" last-event-id)
    (let [event-channel (events conn queue-id last-event-id false)
          [res ch]      (async/alts! [kill-channel event-channel]
                                     :priority true)]
      (cond
        ;; handle errors / kill signal / invalid use
        (= ch kill-channel) (trace "kill signal received")
        (retry-failure? res)
        (do
          (trace "Connection failed,"
                 (if reconnect? "retrying" "closing stream"))
          (if reconnect?
            (let [out-channel (cancelable-retry kill-channel #(register conn))
                  res         (async/<! out-channel)]
              (when-not (= :killed res)
                (let [{queue-id :queue_id last-event-id :last_event_id} res ]
                  (trace "reconnecting with queue-id" queue-id
                         "and last-event-id" last-event-id)
                  (if (and queue-id last-event-id)
                    ;; successfully reconnected, start again from the top
                    (recur queue-id last-event-id)))))
            ;; if not reconnect?
            res))
        (exception? res)    (do (trace "Caught exception, forwarding")
                                (async/>! publish-channel res))
        ;; otherwise process events
        :else               (let [events (seq (:events res))]
                              (if events
                                (do
                                  (doseq [event events]
                                    ;; skip heartbeat events
                                    (if (= (:type event) "heartbeat")
                                      (trace "Received heartbeat event")
                                      (do (trace "Forwarding event")
                                          (async/>! publish-channel event))))
                                  (recur queue-id (apply max (map :id events))))
                                (recur queue-id last-event-id)))))))

(defn ^:deprecated subscribe-events
  "Continuously issue requests against the events endpoint, updating
  the last-event-id so that each event is only returned once. Returns two
  channels, the first one publishes events. The second is a kill channel.
  If an exception is encountered, it is also passed through the publisher
  channel, and the channels will be closed afterwards.
  Use `register` to obtain a event queue.

  Deprecated, use `event-queue` instead, it automatically reconnects"
  ([conn {queue-id :queue_id last-event-id :last_event_id}]
   (subscribe-events conn queue-id last-event-id))
  ([conn queue-id last-event-id]
   (let [publish-channel (async/chan)
         kill-channel (async/chan)]
     (trace "Starting new event subscription loop")
     (subscribe-events* conn queue-id last-event-id
                        publish-channel kill-channel nil)
     [publish-channel kill-channel])))

(defn event-queue
  "Create a durable event publisher.
  Returns two channels, the first one publishes a stream of messages. The second
  can be used to close the stream (by putting anything into it).
  Attempts to recover from server failures by re-trying and re-registering"
  [conn]
  (trace "Creating new durable event queue")
  (let [publish-channel (async/chan)
        kill-channel (async/chan)
        registration (async/<!! (register conn))]
    (subscribe-events* conn (:queue_id registration)
                       (:last_event_id registration)
                       publish-channel kill-channel true)
    [publish-channel kill-channel]))

;; utility functions

(defmacro sync*
  "Wrapper macro to make a request synchronously."
  [& body]
  `(async/<!! (do ~@body)))
