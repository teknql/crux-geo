(ns teknql.crux-geo-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [crux.api :as crux]
            [teknql.crux-geo :as sut]
            [crux.db :as db]
            [clojure.java.io :as io]))

(def ^:dynamic *node* nil)

(defn geo-fixture
  [f]
  (binding [*node* (crux/start-node {:teknql.crux-geo/geo-store {}})]
    (f)
    (.close *node*)))

(defn submit+await-tx
  [tx]
  (->> (crux/submit-tx *node* tx)
       (crux/await-tx *node*)))

(def cities
  [{:crux.db/id :db.id/new-york
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-74.0060 40.7128]}}
   {:crux.db/id :db.id/boston
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-71.0589 42.3601]}}
   {:crux.db/id :db.id/chicago
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-87.6298 41.8781]}}])

(def ny-bounding-box
  {:geometry/type :geometry.type/polygon
   :geometry/coordinates
   [[[-74.04853820800781 40.6920928987952]
     [-73.92013549804688 40.6920928987952]
     [-73.92013549804688 40.78834006798032]
     [-74.04853820800781 40.78834006798032]
     [-74.04853820800781 40.6920928987952]]]})

(use-fixtures :each geo-fixture)

(deftest ->geo->geo-map-test
  (testing "Point"
    (let [point (-> cities first :city/location)
          geo   (#'sut/->geo point)]
      (is (= point (#'sut/->geo-map geo)))))
  (testing "MultiPoint"
    (let [multi {:geometry/type        :geometry.type/multi-point
                 :geometry/coordinates [[100.0, 0.0], [101.0, 1.0]]}
          geo   (#'sut/->geo multi)]
      (is (= multi (#'sut/->geo-map geo)))))
  (testing "LineString"
    (let [line {:geometry/type        :geometry.type/line-string
                :geometry/coordinates [[100.0, 0.0], [101.0, 1.0]]}
          geo  (#'sut/->geo line)]
      (is (= line (#'sut/->geo-map geo)))))
  (testing "Polygon"
    (let [geo (#'sut/->geo ny-bounding-box)]
      (is (= ny-bounding-box (#'sut/->geo-map geo)))))
  (testing "MultiPolygon"
    (let [multi-p {:geometry/type :geometry.type/multi-polygon
                   :geometry/coordinates
                   [(:geometry/coordinates ny-bounding-box)]}
          geo     (#'sut/->geo multi-p)]
      (is (= multi-p (#'sut/->geo-map geo))))))

(deftest intersects-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (testing "returns intersecting geometries"
    (let [result (crux/q
                   (crux/db *node*)
                   '{:find  [?city]
                     :in    [bounding-box]
                     :where [[(geo-intersects :city/location bounding-box) [[?city]]]]}
                   ny-bounding-box)]
      (is (= #{[:db.id/new-york]} result)))))

(deftest nearest-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (testing "returns the nearest item"
    (let [result (crux/q
                   (crux/db *node*)
                   '{:find     [?city ?nearest]
                     :order-by [[?city :asc]]
                     :where    [[?city :city/location ?loc]
                                [(geo-nearest :city/location ?loc) [[?nearest]]]]})]
      (is (= result
             [[:db.id/boston :db.id/new-york]
              [:db.id/chicago :db.id/new-york]
              [:db.id/new-york :db.id/boston]]))))

  (testing "returning multiple items"
    (is (= [[:db.id/boston] [:db.id/chicago]]
           (crux/q
             (crux/db *node*)
             '{:find     [?nearest]
               :order-by [[?nearest :asc]]
               :where    [[?ny :crux.db/id :db.id/new-york]
                          [?ny :city/location ?loc]
                          [(geo-nearest :city/location ?loc 2) [[?nearest]]]]})))))

(deftest persistence-test
  (let [node-cfg {:crux/index-store
                  {:kv-store {:crux/module 'crux.rocksdb/->kv-store
                              :db-dir      (io/file "/tmp/rocksdb")}}
                  :teknql.crux-geo/geo-store {}}]
    (binding [*node* (crux/start-node node-cfg)]
      (submit+await-tx (for [city cities] [:crux.tx/put city]))
      (.close *node*))

    (binding [*node* (crux/start-node node-cfg)]
      (is (= [[:db.id/boston]]
             (crux/q
               (crux/db *node*)
               '{:find     [?nearest]
                 :order-by [[?nearest :asc]]
                 :where    [[?ny :crux.db/id :db.id/new-york]
                            [?ny :city/location ?loc]
                            [(geo-nearest :city/location ?loc) [[?nearest]]]]})))
      (.close *node*))))
