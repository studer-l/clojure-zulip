(ns clojure-zulip.client-test
  (:require [clojure-zulip.client :as sut]
            [clj-http.client :refer [get]]
            [clojure.test :as t]
            [clojure-zulip.core :as zulip]))

(def config {:username "test_bot"
             :api-key "nonewhatsoever"
             :base-url "https://chat.test.org/api/v1"})

(def connection (zulip/connection config))

(t/deftest request-opts
  (t/testing "processes arguments for use by request"
    (t/is (= {:connection-opts config
              :http-fn get
              :arg-symbol :query-params}
             (sut/request-opts :GET connection)))))
