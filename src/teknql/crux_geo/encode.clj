(ns teknql.crux-geo.encode
  "Namespace for dealing with encoding / decoding"
  (:import [org.locationtech.jts.geom Geometry Point GeometryFactory PrecisionModel
            Coordinate LinearRing Polygon MultiPoint MultiPolygon LineString]
           [org.locationtech.jts.io WKTWriter WKTReader]))

(def ^:private geo-factory
  (GeometryFactory. (PrecisionModel.) 4326))

(def ^:private writer
  "Static writer for WKT"
  (WKTWriter.))

(def ^:private reader
  "Static reader for WKT"
  (WKTReader.))

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

(defn map->geo
  "Return the provided map as a geometry"
  (^Geometry
   [m]
   (map->geo geo-factory m))
  (^Geometry
   [^GeometryFactory geo-factory m]
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

(defn geo->map
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
                                (geo->map (.getGeometryN geo n))]]
                      coords))}))

(defn truncate
  "Function to truncate overly precise geometries."
  ([m] (truncate m 10))
  ([m num-decimals]
   (let [scale (Math/pow 10 num-decimals)]
     (-> m
         (update :geometry/coordinates
                 #(mapv (fn truncator [x]
                         (if (vector? x)
                           (mapv truncator x)
                           (/ (Math/round (* x scale)) scale))) %))))))

(defn geo->wkt
  "Convert a Geometry object into WKT representation"
  ^String
  [^Geometry geo]
  (.write ^WKTWriter writer geo))

(defn wkt->geo
  "Convert WKT Text into a Geometry"
  ^Geometry
  [^String wkt-text]
  (.read ^WKTReader reader wkt-text))

(def map->wkt
  "Convert a geometry map into WKT"
  ^String
  (comp geo->wkt map->geo))

(def wkt->map
  "Convert a WKT string into a geometry map"
  (comp geo->map wkt->geo))
