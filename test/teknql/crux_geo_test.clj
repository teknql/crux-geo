(ns teknql.crux-geo-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [crux.api :as crux]
            [clojure.java.io :as io]))

(def ^:dynamic *node* nil)

(defn geo-fixture
  [f]
  (binding [*node* (crux/start-node
                     {:teknql.crux-geo/geo-store
                      {:backend {:crux/module 'teknql.crux-geo.jts/->backend}}})]
    (f)
    (.close *node*)))

(defn submit+await-tx
  [tx]
  (->> (crux/submit-tx *node* tx)
       (crux/await-tx *node*)))

(def cities
  [{:crux.db/id :city.id/new-york
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-74.0060 40.7128]}}
   {:crux.db/id :city.id/boston
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-71.0589 42.3601]}}
   {:crux.db/id :city.id/chicago
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

(deftest intersects-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (testing "returns intersecting geometries"
    (let [result (crux/q
                   (crux/db *node*)
                   '{:find  [?city]
                     :in    [bounding-box]
                     :where [[(geo-intersects :city/location bounding-box) [[?city]]]]}
                   ny-bounding-box)]
      (is (= #{[:city.id/new-york]} result)))))

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
             [[:city.id/boston :city.id/new-york]
              [:city.id/chicago :city.id/new-york]
              [:city.id/new-york :city.id/boston]]))))

  (testing "returning multiple items"
    (is (= [[:city.id/boston] [:city.id/chicago]]
           (crux/q
             (crux/db *node*)
             '{:find     [?nearest]
               :order-by [[?nearest :asc]]
               :where    [[?ny :crux.db/id :city.id/new-york]
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
      (is (= #{[:city.id/boston]}
             (crux/q
               (crux/db *node*)
               '{:find  [?nearest]
                 :where [[?ny :crux.db/id :city.id/new-york]
                         [?ny :city/location ?loc]
                         [(geo-nearest :city/location ?loc) [[?nearest]]]]})))
      (.close *node*))))

(deftest update-test
  (submit+await-tx (for [city cities] [:crux.tx/put city
                                       #inst "2021-05-28T00:00"]))
  (testing "allows querying across time"
    (submit+await-tx
      [[:crux.tx/put
        {:crux.db/id :db.id/car
         :car/location
         {:geometry/type        :geometry.type/point
          :geometry/coordinates [-73.874794 40.715855]}}
        #inst "2021-05-28T12:00"]
       [:crux.tx/put
        {:crux.db/id :db.id/car
         :car/location
         {:geometry/type        :geometry.type/point
          :geometry/coordinates [-71.434160 42.369838]}}
        #inst "2021-05-28T14:00"]])

    (let [nearest-at-start
          (crux/q
            (crux/db *node* #inst "2021-05-28T13:00")
            '{:find  [?nearest]
              :where [[?car :car/location ?loc]
                      [(geo-nearest :city/location ?loc) [[?nearest]]]]})
          nearest-now
          (crux/q
            (crux/db *node*)
            '{:find  [?nearest]
              :where [[?car :car/location ?loc]
                      [(geo-nearest :city/location ?loc) [[?nearest]]]]})]
      (is (= #{[:city.id/new-york]} nearest-at-start))
      (is (= #{[:city.id/boston]} nearest-now)))

    (submit+await-tx
      [[:crux.tx/evict :city.id/boston]])

    (is (= #{[:city.id/new-york]}
           (crux/q
             (crux/db *node*)
             '{:find  [?nearest]
               :where [[?car :car/location ?loc]
                       [(geo-nearest :city/location ?loc) [[?nearest]]]]})))))
