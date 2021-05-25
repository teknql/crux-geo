(ns teknql.crux-geo-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [crux.api :as crux]))

(def ^:dynamic *node* nil)

(defn geo-fixture
  [f]
  (binding [*node* (crux/start-node {:teknql.crux-geo/geo-store {}})]
    (f)))

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



(use-fixtures :each geo-fixture)

(deftest nearest-test
  (submit+await-tx (for [city cities] [:crux.tx/put city]))
  (time
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
                [:db.id/new-york :db.id/boston]])))))

  (testing "returning multiple items"
    (is (= [[:db.id/boston] [:db.id/chicago]]
           (crux/q
             (crux/db *node*)
             '{:find     [?nearest]
               :order-by [[?nearest :asc]]
               :where    [[?ny :crux.db/id :db.id/new-york]
                          [?ny :city/location ?loc]
                          [(geo-nearest :city/location ?loc 2) [[?nearest]]]]})))))
