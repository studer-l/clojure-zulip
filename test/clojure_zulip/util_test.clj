(ns clojure-zulip.util-test
  (:require [clojure-zulip.util :as sut]
            [clojure.test :as t]))

(def zulip-response {:result "success"
                     :msg    ""
                     :values [:quux :baz]})

(t/deftest retry-failure?
  (t/testing "true for internal server error"
    (t/is (sut/retry-failure?
           (ex-info "Internal server error" {:type :zulip-internal-error}))))
  (t/testing "false for bad request"
    (t/is (not (sut/retry-failure? (ex-info "Bad request"
                                            { :type :zulip-bad-request})))))
  (t/testing "false for bad auth"
    (t/is (not (sut/retry-failure? (ex-info "Unauthorized"
                                            {:type :zulip-unauthorized})))))
  (t/testing "true for any other exception that has a type"
    (t/is (sut/retry-failure? (ex-info "foobar" {:type :ran-out-of-gas}))))
  (t/testing "false for any other exception"
    (t/is (not (sut/retry-failure? (ex-info "unexpected" {:unknown true})))))
  (t/testing "false for anything else (especially zulip responses)"
    (t/is (not (sut/retry-failure? zulip-response)))))
