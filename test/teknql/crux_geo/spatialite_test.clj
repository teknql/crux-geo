(ns teknql.crux-geo.spatialite-test
  (:require [teknql.crux-geo.spatialite :as sut]
            [teknql.crux-geo :as geo]
            [clojure.test :as t :refer [deftest is]]))

(deftest ^:backend/spatialite bootstrap-connection!-test
  (with-open [conn (sut/bootstrap-connection!)]
    (is  (some? (#'sut/-sql-query conn ["SELECT spatialite_version() as v"])))))

(deftest ^:backend/spatialite intersects-test
  (with-open [be (sut/->backend {})]
    (let [loc {:geometry/type        :geometry.type/point
               :geometry/coordinates [-74.0060 40.7128]}]
      (geo/index-av! be :car/location loc)
      (is (= [loc] (geo/intersects be :car/location
                                   {:geometry/type :geometry.type/polygon
                                    :geometry/coordinates
                                    [[[-74.04853820800781 40.6920928987952]
                                      [-73.92013549804688 40.6920928987952]
                                      [-73.92013549804688 40.78834006798032]
                                      [-74.04853820800781 40.78834006798032]
                                      [-74.04853820800781 40.6920928987952]]]}))))))

(deftest ^:backend/spatialite nearest-neighbors-test
  (with-open [be (sut/->backend {})]
    (let [loc     {:geometry/type        :geometry.type/point
                   :geometry/coordinates [-74.0060 40.7128]}
          nearest {:geometry/type        :geometry.type/point
                   :geometry/coordinates [-71.0589 42.3601]}
          alt     {:geometry/type        :geometry.type/point
                   :geometry/coordinates [-87.6298 41.8781]}]
      (doseq [place [loc nearest alt]]
        (geo/index-av! be :car/location place))
      (is (= [nearest] (geo/nearest-neighbors be :car/location loc 1))))))
