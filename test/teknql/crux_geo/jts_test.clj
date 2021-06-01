(ns teknql.crux-geo.jts-test
  (:require [teknql.crux-geo.jts :as sut]
            [clojure.test :as t :refer [deftest testing is]]
            [teknql.crux-geo-test :as geo-test]))

(deftest ->geo->geo-map-test
  (testing "Point"
    (let [point (-> geo-test/cities first :city/location)
          geo   (sut/->geo point)]
      (is (= point (sut/->geo-map geo)))))
  (testing "MultiPoint"
    (let [multi {:geometry/type        :geometry.type/multi-point
                 :geometry/coordinates [[100.0, 0.0], [101.0, 1.0]]}
          geo   (sut/->geo multi)]
      (is (= multi (sut/->geo-map geo)))))
  (testing "LineString"
    (let [line {:geometry/type        :geometry.type/line-string
                :geometry/coordinates [[100.0, 0.0], [101.0, 1.0]]}
          geo  (sut/->geo line)]
      (is (= line (sut/->geo-map geo)))))
  (testing "Polygon"
    (let [geo (sut/->geo geo-test/ny-bounding-box)]
      (is (= geo-test/ny-bounding-box (sut/->geo-map geo)))))
  (testing "MultiPolygon"
    (let [multi-p {:geometry/type :geometry.type/multi-polygon
                   :geometry/coordinates
                   [(:geometry/coordinates geo-test/ny-bounding-box)]}
          geo     (sut/->geo multi-p)]
      (is (= multi-p (sut/->geo-map geo))))))
