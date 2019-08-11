(ns clojure-zulip.client
  (:require [clojure.tools.logging :refer [error]]
            [clj-http.client :as http]
            [clojure.core.async :as async]
            [cheshire.core :as cheshire]))

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
  (try
    (let [result (http-fn (uri (:base-url connection-opts) endpoint)
                          {:basic-auth     [(:username connection-opts)
                                            (:api-key connection-opts)]
                           :socket-timeout (* 90 1000) ;; 1.5 mins in millis
                           arg-symbol      request-args})]
      (extract-body result))
    (catch clojure.lang.ExceptionInfo ex
      (let [data (ex-data ex)]
        (case (:status data)
          ;; Bad Request
          400 (let [zulip-error (extract-body ex)]
                (log-exception verb (uri (:base-url connection-opts) endpoint)
                               request-args zulip-error)
                (ex-info "Bad request" (assoc zulip-error
                                              :type :zulip-bad-request)))
          ;; Forbidden
          401 (do (log-exception verb (uri (:base-url connection-opts) endpoint)
                                 request-args "401 Unauthorized")
                  (ex-info "Unauthorized" {:type :zulip-unauthorized}))
          ;; Internal server error; Occasionally happens at off times
          ;; (like 6 in the morning on Sundays?!)
          500 (do (log-exception verb (uri (:base-url connection-opts) endpoint)
                                 request-args "500 Internal server error")
                  (ex-info "Internal server error" {:type :zulip-internal-error}))
          ;; Bad gateway; This is the "Zulip server is experiencing some
          ;; technical difficulties" page that /sometimes/ shows up
          502 (do (log-exception verb (uri (:base-url connection-opts) endpoint)
                                 request-args "502 Bad gateway")
                  (ex-info "Bad gateway" {:type :zulip-bad-gateway}))
          ;; not handled clj-http exception
          (do (log-exception verb (uri (:base-url connection-opts)
                                       endpoint)
                             request-args ex)
              ex))))
    (catch java.net.SocketTimeoutException ex
      (log-exception verb (uri (:base-url connection-opts) endpoint)
                     request-args "java.net.SocketException")
      (ex-info "Timeout" {:type :zulip-timeout :endpoint endpoint}))
    (catch java.net.UnknownHostException ex
      (log-exception verb (uri (:base-url connection-opts) endpoint)
                     request-args "java.net.UnknownHostException")
      (ex-info "Unknown Host" {:type :zulip-unknown-host :endpoint endpoint}))
    (catch Exception ex
      ;; in any other case, put raw exception in channel
      (log-exception verb (uri (:base-url connection-opts) endpoint)
                     request-args ex)
      (ex-info "Exception" {:type     :generic-exception
                            :endpoint endpoint :exception ex}))))

(defn request
  "Issue a request to the Zulip API. Accepted verbs are :GET, :POST,
  and :PATCH. Return a channel to which the response body will be
  written."
  ([verb connection endpoint] (request verb connection endpoint {}))
  ([verb connection endpoint request-args]
   (let [{:keys [connection-opts http-fn arg-symbol]} (request-opts verb connection)]
     (async/thread
       (blocking-request connection-opts http-fn endpoint
                         verb arg-symbol request-args)))))
