(ns teknql.crux-geo
  (:require [crux.bus :as bus]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.query :as q]
            [crux.system :as sys]
            [clojure.spec.alpha :as s])
  (:import [crux.query VarBinding]
           [org.locationtech.jts.index.strtree STRtree GeometryItemDistance]
           [org.locationtech.jts.geom Geometry Point GeometryFactory PrecisionModel Coordinate]))

(defrecord DocID [a v])

(def ^:private geo-factory
  (GeometryFactory. (PrecisionModel.) 4326))

(defn- ->geo
  "Return the provided map as a geometry"
  [^GeometryFactory geo-factory m] ^Geometry
  (when (map? m)
    (let [coords (:geometry/coordinates m)]
      (case (:geometry/type m)
        :geometry.type/point (.createPoint geo-factory
                                           (Coordinate. (nth coords 0) (nth coords 1)))
        nil))))

(defn- ->geo-map
  "Return the provided Geometry as a map"
  [geo]
  (cond
    (instance? Point geo) {:geometry/type        :geometry.type/point
                           :geometry/coordinates [(.. geo getCoordinate -x)
                                                  (.. geo getCoordinate -y)]}))

(defn geo?
  "Return whether the provided map `m` can be interprited as a geometry"
  [m]
  (some? (->geo geo-factory m)))

(defn a->rtree "Atomically grab the rtree from the provided ar-tree* atom"
  [ar-tree* a] ^STRtree
  (swap! ar-tree* update a #(or % (STRtree.)))
  (get @ar-tree* a))

(defn ->id-str
  [a v]
  (str (cc/new-id (DocID. a v))))

(defn index!
  "Index the provided documents into the passed in r-tree"
  [ar-tree* ^GeometryFactory geo-factory docs]
  (doseq [[_id doc] docs
          [k v]     doc
          :let      [geo (->geo geo-factory v)]
          :when     geo
          :let      [r-tree (a->rtree ar-tree* k)]]
    (.insert r-tree (.getEnvelopeInternal geo) geo)))


(defmethod q/pred-args-spec 'geo-nearest [_]
  (s/cat :pred-fn  #{'geo-nearest}
         :args (s/spec (s/cat :attr keyword? :v (some-fn geo? symbol?)))
         :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'geo-nearest [_ pred-ctx]
  (let [{:keys [::geo-store
                arg-bindings
                return-type
                tuple-idxs-in-join-order
                idx-id]} pred-ctx
        attr             (second arg-bindings)
        r-tree           (a->rtree (:r-trees geo-store) attr)
        geo-fac          (:geo-factory geo-store)
        geo-dist         (GeometryItemDistance.)]
    (fn pred-get-attr-constraint [index-snapshot
                                  {:keys [entity-resolver-fn]}
                                  idx-id->idx join-keys]
      (let [[v n] (->> arg-bindings
                       (drop 2)
                       (map #(if (instance? VarBinding %)
                               (q/bound-result-for-var index-snapshot % join-keys)
                               %)))]
        (when-some [geo (->geo geo-fac v)]
          (->> (for [neighbor     (.nearestNeighbour r-tree
                                                     (.getEnvelopeInternal geo)
                                                     geo
                                                     geo-dist
                                                     (inc (or n 1)))
                     :when        (not= geo neighbor)
                     :let         [neighbor-map (->geo-map neighbor)]
                     neighbor-eid (db/ave index-snapshot attr neighbor-map nil entity-resolver-fn)]
                 [(db/decode-value index-snapshot neighbor-eid) neighbor-map])
               (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id))))))))

(defn ->geo-store
  {::sys/args   {:srid      {:doc       "Spatial Reference Identifier"
                             :required? false
                             :default   4326
                             :spec      pos-int?}
                 :precision {:doc       "The Precision to use"
                             :required? false
                             :default   :floating
                             :spec      keyword?}}
   ::sys/deps   {:bus            :crux/bus
                 :document-store :crux/document-store
                 :index-store    :crux/index-store
                 :query-engine   :crux/query-engine}
   ::sys/before #{[:crux/tx-ingester]}}
  [{:keys [document-store bus query-engine srid precision]}]
  (let [ar-tree*    (atom {})
        geo-factory (GeometryFactory.
                      (case precision
                        :floating (PrecisionModel.))
                      srid)]
    (q/assoc-pred-ctx! query-engine ::geo-store {:r-trees     ar-tree*
                                                 :geo-factory geo-factory})
    (bus/listen bus {:crux/event-types  #{:crux.tx/committing-tx :crux.tx/aborting-tx}
                     :crux.bus/executor (reify java.util.concurrent.Executor
                                          (execute [_ f]
                                            (.run f)))}
                #(index! ar-tree* geo-factory (db/fetch-docs document-store (:doc-ids %))))
    ar-tree*))

(comment
  (count (->id-str :location 5)))
