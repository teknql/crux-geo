(ns teknql.crux-geo
  (:require [crux.bus :as bus]
            [crux.system :as sys]
            [crux.db :as db]
            [crux.query :as q]
            [clojure.spec.alpha :as s])
  (:import [crux.query VarBinding]))


(defprotocol GeoBackend
  "Abstract backend used to power crux-geo"
  (index-av! [backend attr v])
  (evict-av! [backend attr v])
  (nearest-neighbors [backend attr v n])
  (intersects [backend attr v]))


(defn geo?
  "Return whether the provided map `m` appears to be a geometry."
  [m]
  (#{:geometry.type/point
     :geometry.type/multi-point
     :geometry.type/line-string
     :geometry.type/polygon
     :geometry.type/multi-polygon} (:geometry/type m)))

(defn- -index!
  "Index the provided documents into the passed in r-tree"
  [{:keys [backend
           index-store
           known-attrs]} docs]
  (doseq [[_id doc] docs
          [k v]     doc]
    (when (geo? v)
      (when-not (@known-attrs k)
        (db/store-index-meta index-store ::known-attrs
                             (swap! known-attrs conj k)))
      (index-av! backend k v))))

(defn- -evict!
  "Evict the provided document IDs"
  [{:keys [index-store
           backend]} eids]
  (with-open [index-snapshot (db/open-index-snapshot index-store)]
    (doseq [[a v] (db/exclusive-avs index-store eids)
            :let  [a (db/decode-value index-snapshot a)
                   v (db/decode-value index-snapshot v)]]
      (evict-av! backend a v))))


(defmethod q/pred-args-spec 'geo-nearest [_]
  (s/cat :pred-fn  #{'geo-nearest}
         :args (s/spec (s/cat :attr keyword? :v (some-fn geo? symbol?)))
         :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'geo-nearest [_ pred-ctx]
  (let [{:keys [::backend
                arg-bindings
                return-type
                tuple-idxs-in-join-order
                idx-id]} pred-ctx
        attr             (second arg-bindings)]
    (fn pred-get-attr-constraint [index-snapshot
                                  {:keys [entity-resolver-fn]}
                                  idx-id->idx join-keys]
      (let [[v n] (->> arg-bindings
                       (drop 2)
                       (map #(if (instance? VarBinding %)
                               (q/bound-result-for-var index-snapshot % join-keys)
                               %)))]
        (->> (for [neighbor     (nearest-neighbors backend attr v (or n 1))
                   neighbor-eid (db/ave index-snapshot attr neighbor nil entity-resolver-fn)]
               [(db/decode-value index-snapshot neighbor-eid) neighbor])
             (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id)))))))


(defmethod q/pred-args-spec 'geo-intersects [_]
  (s/cat :pred-fn  #{'geo-intersects}
         :args (s/spec (s/cat :attr keyword? :v (some-fn geo? symbol?)))
         :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'geo-intersects [_ pred-ctx]
  (let [{:keys [::backend
                arg-bindings
                return-type
                tuple-idxs-in-join-order
                idx-id]} pred-ctx
        attr             (second arg-bindings)]
    (fn pred-get-attr-constraint [index-snapshot
                                  {:keys [entity-resolver-fn]}
                                  idx-id->idx join-keys]
      (let [[v] (->> arg-bindings
                     (drop 2)
                     (map #(if (instance? VarBinding %)
                             (q/bound-result-for-var index-snapshot % join-keys)
                             %)))]
        (->> (for [neighbor     (intersects backend attr v)
                   neighbor-eid (db/ave index-snapshot attr neighbor nil entity-resolver-fn)]
               [(db/decode-value index-snapshot neighbor-eid) neighbor])
             (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id)))))))

(defn ->geo-store
  {::sys/deps   {:bus            :crux/bus
                 :document-store :crux/document-store
                 :index-store    :crux/index-store
                 :query-engine   :crux/query-engine
                 :backend        'teknql.crux-geo.jts/->backend}
   ::sys/before #{[:crux/tx-ingester]}}
  [{:keys [document-store bus query-engine index-store backend]}]
  (assert (satisfies? GeoBackend backend) "Invalid Backend provided")
  (let [ctx {:backend     backend
             :known-attrs (atom (set (db/read-index-meta index-store ::known-attrs)))
             :index-store index-store}]
    (q/assoc-pred-ctx! query-engine ::backend backend)
    (bus/listen bus {:crux/event-types  #{:crux.tx/committing-tx :crux.tx/aborting-tx}
                     :crux.bus/executor (reify java.util.concurrent.Executor
                                          (execute [_ f]
                                            (.run f)))}
                (fn [x]
                  (-index! ctx (db/fetch-docs document-store (:doc-ids x)))
                  (when-some [eids (not-empty (:evicting-eids x))]
                    (-evict! ctx eids))))))
