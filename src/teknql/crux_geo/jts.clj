(ns teknql.crux-geo.jts
  "Java Topology Suite Backend for crux-geo"
  (:require [crux.system :as sys]
            [crux.db :as db]
            [teknql.crux-geo :refer [GeoBackend]]
            [teknql.crux-geo.encode :as encode])
  (:import [org.locationtech.jts.index.strtree STRtree GeometryItemDistance]
           [org.locationtech.jts.geom Geometry]))


(def ^:private geo-dist
  (GeometryItemDistance.))

(defn- a->r-tree
  "Accessor to either instantiate or get an existing r-tree from the provided backend
  for attribute `a`"
  ^STRtree
  [r-trees a]
  (swap! r-trees update a #(or % (STRtree.)))
  (get @r-trees a))

(defrecord JTSBackend [r-trees]
  GeoBackend
  (index-av! [_ attr v]
    (let [r-tree (a->r-tree r-trees attr)
          geo    (encode/map->geo v)]
      (.insert r-tree (.getEnvelopeInternal geo) geo)))
  (evict-av! [_ attr v]
    (let [r-tree (a->r-tree r-trees attr)
          geo    (encode/map->geo v)
          env    (.getEnvelopeInternal geo)]
      (doseq [^Geometry item (.query r-tree env)
              :when          (.equals item geo)]
        (.remove r-tree env item))))
  (nearest-neighbors [_ attr v n]
    (let [r-tree (a->r-tree r-trees attr)
          geo    (encode/map->geo v)]
      (->> (.nearestNeighbour
             r-tree
             (.getEnvelopeInternal geo)
             geo
             geo-dist
             (inc n))
           (remove #(.equals geo %))
           (take n)
           (map encode/geo->map))))
  (intersects [_ attr v]
    (let [r-tree (a->r-tree r-trees attr)
          geo    (encode/map->geo v)]
      (->> (.query r-tree (.getEnvelopeInternal geo))
           (remove #(.equals geo %))
           (map encode/geo->map)))))

(defn ->backend
  "Return a JTS-based Driver for crux-geo"
  {::sys/deps {:index-store :crux/index-store}}
  [{:keys [index-store]}]
  (->JTSBackend
    (atom
      (let [attrs (db/read-index-meta index-store :teknql.crux-geo/known-attrs)]
        (->> (with-open [index-snapshot (db/open-index-snapshot index-store)]
               (doall
                 (for [attr attrs
                       :let [r-tree (STRtree.)]]
                   (do (doseq [v     (db/av index-snapshot attr nil)
                               :let  [v (db/decode-value index-snapshot v)
                                      geo (encode/map->geo v)]
                               :when geo]
                         (.insert r-tree (.getEnvelopeInternal geo) geo))
                       [attr r-tree]))))
             (into {}))))))
