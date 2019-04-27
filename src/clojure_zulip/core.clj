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

;; higher-level convenience functions built on top of basic API commands

(defn- timeout-exception? [r]
  (some-> r ex-data :type (= :zulip-timeout)))

(defn- subscribe-events*
  "Launch a goroutine that continuously publishes events on the
  publish-channel until an exception is encountered, the connection
  is closed or the kill-channel receives an event.

  Returns reason for closing event loop."
  [conn queue-id last-event-id publish-channel kill-channel]
  (async/go-loop [last-event-id last-event-id]
    (debug "Waiting for new events, last-event-id =" last-event-id)
    (let [event-channel (events conn queue-id last-event-id false)
          [result ch] (async/alts! [kill-channel event-channel]
                                   :priority true)]
      (cond
        ;; handle errors / kill signal / invalid use
        (= ch kill-channel) (do (info "kill signal received")
                                :kill)
        (nil? result) (do (info "publisher closed")
                          :close)
        (timeout-exception? result) (do (info "Polling timeout")
                                        :timeout)
        (instance? Exception result) (do (info "Caught exception, forwarding")
                                         (async/>! publish-channel result))
        :else ;; otherwise process events
        (let [events (seq (:events result))]
          (if events
            (do
              (doseq [event events]
                ;; skip heartbeat events
                (if (= (:type event) "heartbeat")
                  (trace "Received heartbeat event")
                  (do (trace "Forwarding event")
                      (async/>! publish-channel event))))
              (recur (apply max (map :id events))))
            (recur last-event-id)))))))

(defn subscribe-events
  "Continuously issue requests against the events endpoint, updating
  the last-event-id so that each event is only returned once. Returns two
  channels, the first one publishes events. The second is a kill channel.
  If an exception is encountered, it is also passed through the publisher
  channel, and the channels will be closed afterwards.
  Use `register` to obtain a event queue."
  ([conn {queue-id :queue_id last-event-id :last_event_id}]
   (subscribe-events conn queue-id last-event-id))
  ([conn queue-id last-event-id]
   (let [publish-channel (async/chan)
         kill-channel (async/chan)]
     (trace "Starting new event subscription loop")
     (subscribe-events* conn queue-id last-event-id
                        publish-channel kill-channel)
     [publish-channel kill-channel])))

;; utility functions

(defmacro sync*
  "Wrapper macro to make a request synchronously."
  [& body]
  `(async/<!! (do ~@body)))
