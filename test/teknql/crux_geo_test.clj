(ns teknql.crux-geo-test
  (:require [clojure.test :refer [deftest testing is]]
            [crux.api :as crux]
            [clojure.string :as str]
            [teknql.crux-geo.encode :as encode])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *node* nil)

(def ^:dynamic *backend* nil)

(defmacro def-backend-test
  [name & body]
  (let [backends ['teknql.crux-geo.jts/->backend
                  'teknql.crux-geo.spatialite/->backend]
        backend->name #(last (str/split (namespace %) #"\."))
        tests (map (fn [backend]
                    (let [backend-meta (keyword "backend" (backend->name backend))
                          test-symbol  (symbol (str (backend->name backend) "-" name))]
                      `(deftest ~(with-meta test-symbol {backend-meta true})
                         (with-open [node# (crux/start-node
                                             {:teknql.crux-geo/geo-store
                                              {:backend {:crux/module  '~backend}}})]
                           (binding [*node*    node#
                                     *backend* '~backend]
                             ~@body))))) backends)]
    `(do ~@tests)))

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

(def-backend-test intersects-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (testing "returns intersecting geometries"
    (let [result (crux/q
                   (crux/db *node*)
                   '{:find  [?city]
                     :in    [bounding-box]
                     :where [[(geo-intersects :city/location bounding-box) [[?city]]]]}
                   ny-bounding-box)]
      (is (= #{[:city.id/new-york]} result)))))


(def-backend-test nearest-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (testing "returns the nearest item"
    (let [result (crux/q
                   (crux/db *node*)
                   '{:find     [?city ?nearest]
                     :order-by [[?city :asc]]
                     :where    [[?city :city/location ?loc]
                                [(geo-nearest :city/location ?loc) [[?nearest]]]]})]
      (is (= [[:city.id/boston :city.id/new-york]
              [:city.id/chicago :city.id/new-york]
              [:city.id/new-york :city.id/boston]]
             result))))

  (testing "returning multiple items"
    (is (= [[:city.id/boston] [:city.id/chicago]]
           (crux/q
             (crux/db *node*)
             '{:find     [?nearest]
               :order-by [[?nearest :asc]]
               :where    [[?ny :crux.db/id :city.id/new-york]
                          [?ny :city/location ?loc]
                          [(geo-nearest :city/location ?loc 2) [[?nearest]]]]})))))

(def-backend-test persistence-test
  (let [db-dir   (Files/createTempDirectory "crux-geo-store-persistence-test-db"
                                            (into-array FileAttribute []))
        be-opts (condp = *backend*
                  'teknql.crux-geo.jts/->backend
                  {:crux/module *backend*}
                  'teknql.crux-geo.spatialite/->backend
                  {:crux/module *backend*
                   :db-path
                   (str (Files/createTempFile "crux-geo-store-persistence-test-db"
                                              ".sqlite"
                                              (into-array FileAttribute [])))})
        node-cfg {:crux/index-store
                  {:kv-store {:crux/module 'crux.rocksdb/->kv-store
                              :db-dir      db-dir}}
                  :teknql.crux-geo/geo-store {:backend be-opts}}]
    (with-open [node (crux/start-node node-cfg)]
      (binding [*node* node]
        (submit+await-tx (for [city cities] [:crux.tx/put city]))))

    (with-open [node (crux/start-node node-cfg)]
      (binding [*node* node]
        (is (= #{[:city.id/boston]}
               (crux/q
                 (crux/db *node*)
                 '{:find  [?nearest]
                   :where [[?ny :crux.db/id :city.id/new-york]
                           [?ny :city/location ?loc]
                           [(geo-nearest :city/location ?loc) [[?nearest]]]]})))))))

(def-backend-test update-test
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

(def-backend-test overly-precise-test
  (testing "supports extremely precise geometries"
    (let [person {:crux.db/id      :person/peter
                  :person/location {:geometry/type        :geometry.type/point
                                    :geometry/coordinates [-73.9800585141354980468
                                                           40.71250158910928987952]}}]
      (submit+await-tx [[:crux.tx/put person]])
      (let [result (crux/q
                     (crux/db *node*)
                     '{:find  [?person]
                       :in    [bounding-box]
                       :where [[(geo-intersects :person/location bounding-box) [[?person]]]]}
                     ny-bounding-box)]
        (is (= #{[:person/peter]} result))))))
