(ns echo-bot
  (:require [clojure-zulip.core :as zulip]
            [clojure.string :as str]
            [clojure.core.async :as async]))

(def conn (zulip/connection
           {:username "echo-bot@zulip.com"
            :api-key "secret api key"
            :base-url "https://chat.zulip.com/api/v1"}))

(defn handle-event
  "Check whether event contains a message starting with '!echo' if yes,
  reply (either in private or on stream) with the rest of the message."
  [conn event]
  (let [message (:message event)
        {stream :display_recipient
         message-type :type
         sender :sender_email
         :keys [:subject :content]} message]
    ;; the message is for us if it begins with `!echo`
    (if (and content (str/starts-with? content "!echo "))
      ;; if so, remove leading `!echo`
      (let [reply (subs content 6)]
        ;; Reply to private message in private
        (if (= message-type "private")
          (zulip/send-private-message conn sender reply)
          ;; otherwise post to stream
          (zulip/send-stream-message conn stream subject reply))))))

(defn mk-handler-channel
  "Create channel that calls `handle-event` on input with `conn`"
  [conn]
  (let [c (async/chan)]
    (async/go-loop []
      (handle-event conn (async/<! c))
      (recur))
    c))

(defn mk-echo-bot [conn]
  (let [register-response (zulip/sync* (zulip/register conn))
        [event-channel kill-channel] (zulip/subscribe-events conn register-response)
        handler-channel (mk-handler-channel conn)]
    ;; Connect event input to handler channel
    (async/pipe event-channel handler-channel)
    kill-channel))

(defn stop-echo-bot [kill-channel]
  (async/>!! kill-channel :close))
