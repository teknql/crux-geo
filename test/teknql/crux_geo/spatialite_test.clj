(ns teknql.crux-geo.spatialite-test
  (:require [teknql.crux-geo.spatialite :as sut]
            [teknql.crux-geo :as geo]
            [clojure.test :as t :refer [deftest is testing]]
            [crux.system :as sys]
            [next.jdbc :as jdbc]))

(def ^:dynamic *opts*
  {:db-dir          ":memory:"
   :spatialite-path "/nix/store/czlsm3jgr5s96l8i0hzknpqk3ywppsrp-libspatialite-4.3.0a/lib/mod_spatialite"})

(deftest bootstrap-connection!-test
  (with-open [conn (sut/bootstrap-connection! *opts*)]
    (is (= {:v "4.3.0a"}
           (jdbc/execute-one!
             conn
             ["SELECT spatialite_version() as v"])))))

(deftest intersects-test
  (with-open [be (sut/->backend *opts*)]
    (let [loc {:geometry/type        :geometry.type/point
               :geometry/coordinates [-71.434160 42.369838]}]
      (geo/index-av! be :car/location loc)
      (is (= [loc] (geo/intersects be :car/location
                                   {:geometry/type :geometry.type/polygon
                                    :geometry/coordinates
                                    [[[-74.04853820800781 40.6920928987952]
                                      [-73.92013549804688 40.6920928987952]
                                      [-73.92013549804688 40.78834006798032]
                                      [-74.04853820800781 40.78834006798032]
                                      [-74.04853820800781 40.6920928987952]]]}))))))
