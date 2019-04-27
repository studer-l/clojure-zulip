(ns clojure-zulip.client
  (:require [clojure.tools.logging :refer [error]]
            [clj-http.client :as http]
            [clojure.core.async :as async]
            [cheshire.core :as cheshire]
            [slingshot.slingshot :refer [try+]]))

(defn- uri
  "Construct a URI from route fragments."
  [& more]
  (->> (map #(if (= (last "/") (last %)) % (str % "/")) more)
       (apply str)
       (butlast)
       (apply str)))

(defn extract-body
  "Return the body from an HTTP response, deserialized from JSON if
  applicable."
  [response]
  (if-not (= (get-in response [:headers "content-type"]) "application/json")
    (:body response)
    (cheshire/parse-string (:body response) true)))

(defn request-opts
  "Return dict of processed arguments for use by request."
  [verb connection]
  {:connection-opts (:opts connection)
   :http-fn (case verb
              :GET http/get
              :POST http/post
              :PATCH http/patch)
   :arg-symbol (case verb
                 :GET :query-params
                 :POST :form-params
                 :PATCH :query-params)})

(defn- log-exception
  "For logging exceptions in `request`"
  [verb uri request-args ex]
  (error {:ms (System/currentTimeMillis)
          :method verb
          :uri uri
          :request-args request-args
          :exception ex}))


(defn- blocking-request
  "Perform blocking request; Comes with a timeout of 90 seconds which is
  reasonable for event queue where heartbeat is expected every minute."
  [connection-opts http-fn endpoint verb arg-symbol request-args]
  (try+
   (let [result (http-fn (uri (:base-url connection-opts) endpoint)
                         {:basic-auth [(:username connection-opts)
                                       (:api-key connection-opts)]
                          :socket-timeout (* 90 1000) ;; 1.5 mins in millis
                          arg-symbol request-args})]
     (extract-body result))
   (catch [:status 400] ex
     ;; In case of bad request, parse zulip's error message and put it
     ;; on the channel as IExceptionInfo
     (let [zulip-error (extract-body ex)]
       (log-exception verb (uri (:base-url connection-opts) endpoint)
                      request-args zulip-error)
       (ex-info "Bad request" (assoc zulip-error
                                     :type :zulip-bad-request))))
   (catch java.net.SocketException ex
     (log-exception verb  (uri (:base-url connection-opts) endpoint)
                    request-args "java.net.SocketException")
     (ex-info "Timeout" {:type :zulip-timeout :endpoint endpoint}))
   (catch Exception ex
     ;; in any other case, put raw exception in channel
     (log-exception verb (uri (:base-url connection-opts) endpoint)
                    request-args ex)
     ex)))

(defn request
  "Issue a request to the Zulip API. Accepted verbs are :GET, :POST,
  and :PATCH. Return a channel to which the response body will be
  written."
  ([verb connection endpoint] (request verb connection endpoint {}))
  ([verb connection endpoint request-args]
   (let [{:keys [connection-opts http-fn arg-symbol]} (request-opts verb connection)
         channel (async/chan)]
     (future
       (async/>!! channel (blocking-request connection-opts http-fn endpoint
                                            verb arg-symbol request-args)))
     channel)))
