(ns teknql.crux-geo.encode-test
  (:require [teknql.crux-geo.encode :as sut]
            [clojure.test :refer [deftest testing is]]
            [teknql.crux-geo-test :as geo-test]))

(def sample-data
  [{:name "Point"
    :data {:geometry/type        :geometry.type/point
           :geometry/coordinates [100.0, 0.0]}}
   {:name "MultiPoint"
    :data {:geometry/type        :geometry.type/multi-point
           :geometry/coordinates [[100.0, 0.0] , [101.0, 1.0]]}}
   {:name "LineString"
    :data {:geometry/type        :geometry.type/line-string
           :geometry/coordinates [[100.0, 0.0] , [101.0, 1.0]]}}
   {:name "Polygon"
    :data geo-test/ny-bounding-box}
   {:name "MultiPolygon"
    :data {:geometry/type :geometry.type/multi-polygon
           :geometry/coordinates
           [(:geometry/coordinates geo-test/ny-bounding-box)]}}])


(deftest round-trips-test
  (doseq [[encode decode] [[#'sut/map->wkt #'sut/wkt->map]
                           [#'sut/map->wkb #'sut/wkb->map]]]
    (testing (str (:name (meta encode)) "->" (:name (meta decode)))
      (doseq [{:keys [name data]} sample-data]
        (testing name
          (is (= data (-> data encode decode))))))))
