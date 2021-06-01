(ns teknql.crux-geo.jts
  "Java Topology Suite Backend for crux-geo"
  (:require [crux.system :as sys]
            [crux.db :as db]
            [teknql.crux-geo :refer [GeoBackend]])
  (:import [org.locationtech.jts.index.strtree STRtree GeometryItemDistance]
           [org.locationtech.jts.geom Geometry Point GeometryFactory PrecisionModel
            Coordinate LinearRing Polygon MultiPoint MultiPolygon LineString]))

(def ^:private geo-factory
  (GeometryFactory. (PrecisionModel.) 4326))

(def ^:private geo-dist
  (GeometryItemDistance.))


(defn- ->coordinate
  "Return a coordinate from the provided `x y` vector."
  ^Coordinate
  [[x y]]
  (Coordinate. x y))

(defn- ->coordinates
  "Return a java array of coordinates from the provided seqable of `[x y]` vectors"
  ^"[Lorg.locationtech.jts.geom.Coordinate;"
  [coords]
  (into-array Coordinate (map ->coordinate coords)))

(defn- ->polygon
  "Return a geo polygon from the provided vector of vector of point vectors"
  [^GeometryFactory geo-factory coords]
  (let [linear-rings (map
                       (fn [ring-coords]
                         (.createLinearRing
                           geo-factory
                           (->coordinates ring-coords)))
                       coords)]
    (.createPolygon geo-factory (first linear-rings) (into-array LinearRing (rest linear-rings)))))

(defn ->geo
  "Return the provided map as a geometry"
  ([m] (->geo geo-factory m))
  (^Geometry [^GeometryFactory geo-factory m]
   (when (map? m)
     (let [coords (:geometry/coordinates m)]
       (case (:geometry/type m)
         :geometry.type/point       (.createPoint geo-factory (->coordinate coords))
         :geometry.type/multi-point (.createMultiPoint geo-factory (->coordinates coords))
         :geometry.type/line-string (.createLineString geo-factory (->coordinates coords))
         :geometry.type/polygon     (->polygon geo-factory coords)
         :geometry.type/multi-polygon
         (.createMultiPolygon
           geo-factory (into-array Polygon (map (partial ->polygon geo-factory) coords)))
         nil)))))

(defn- coord->vec
  "Return a coordinate as an x y vector"
  [^Coordinate coord]
  [(.-x coord) (.-y coord)])

(defn ->geo-map
  "Return the provided Geometry as a map"
  [^Geometry geo]
  (condp instance? geo
    Point        {:geometry/type        :geometry.type/point
                  :geometry/coordinates (coord->vec (.getCoordinate geo))}
    MultiPoint   {:geometry/type        :geometry.type/multi-point
                  :geometry/coordinates (mapv coord->vec (.getCoordinates geo))}
    LineString   {:geometry/type        :geometry.type/line-string
                  :geometry/coordinates (mapv coord->vec (.getCoordinates geo))}
    Polygon      {:geometry/type :geometry.type/polygon
                  :geometry/coordinates
                  (let [^Polygon geo geo]
                    (into [(mapv coord->vec (.. geo getExteriorRing getCoordinates))]
                          (for [n (range (.getNumInteriorRing geo))]
                            (mapv coord->vec (.. geo
                                                 (getInteriorRingN n)
                                                 (getCoordinates))))))}
    MultiPolygon {:geometry/type :geometry.type/multi-polygon
                  :geometry/coordinates
                  (vec
                    (for [n    (range (.getNumGeometries geo))
                          :let [{coords :geometry/coordinates}
                                (->geo-map (.getGeometryN geo n))]]
                      coords))}))

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
    (let [r-tree        (a->r-tree r-trees attr)
          ^Geometry geo (->geo v)]
      (.insert r-tree (.getEnvelopeInternal geo) geo)))
  (evict-av! [_ attr v]
    (let [r-tree        (a->r-tree r-trees attr)
          ^Geometry geo (->geo v)
          env           (.getEnvelopeInternal geo)]
      (doseq [^Geometry item (.query r-tree env)
              :when          (.equals item geo)]
        (.remove r-tree env item))))
  (nearest-neighbors [_ attr v n]
    (let [r-tree        (a->r-tree r-trees attr)
          ^Geometry geo (->geo v)]
      (->> (.nearestNeighbour
             r-tree
             (.getEnvelopeInternal geo)
             geo
             geo-dist
             (inc n))
           (remove #(.equals geo %))
           (take n)
           (map ->geo-map))))
  (intersects [_ attr v]
    (let [r-tree        (a->r-tree r-trees attr)
          ^Geometry geo (->geo v)]
      (->> (.query r-tree (.getEnvelopeInternal geo))
           (remove #(.equals geo %))
           (map ->geo-map)))))

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
                                      ^Geometry geo (->geo v)]
                               :when geo]
                         (.insert r-tree (.getEnvelopeInternal geo) geo))
                       [attr r-tree]))))
             (into {}))))))
