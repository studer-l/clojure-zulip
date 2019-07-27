(ns clojure-zulip.util)

(defn retry-failure?
  "Returns truthy if this is a kind of connection failure that can be solved by
  retrying after some delay."
  [r]
  (some->> r
           ex-data
           :type
           (contains? #{:zulip-bad-request :zulip-unauthorized})
           not))

(defn exception? [ex] (instance? Exception ex))
