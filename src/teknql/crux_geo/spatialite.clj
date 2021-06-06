(ns teknql.crux-geo.spatialite
  "Spatialite backend for crux-geo"
  (:require [teknql.crux-geo :refer [GeoBackend]]
            [teknql.crux-geo.jts :refer [->geo ->geo-map]]
            [crux.system :as sys])
  (:import [org.locationtech.jts.io WKTWriter WKTReader]
           [org.locationtech.jts.geom Geometry]
           [org.sqlite Conn Stmt OpenFlags]))

(def ^:private writer
  "Static writer for WKT"
  (WKTWriter.))

(def ^:private reader
  "Static reader for WKT"
  (WKTReader.))

(defn- ->wkt
  "Convert a Geometry object into WKT representation"
  [^Geometry geo]
  (.write ^WKTWriter writer geo))

(defn- wkt->geo
  [^String wkt-text]
  (.read ^WKTReader reader wkt-text))


(defn- -sql-exec!
  "Execute the provided SQL"
  [^Conn conn [sql & args]]
  (with-open [stmt (.prepare conn sql true)]
     (.execDml stmt (to-array args))))

(defn- -stmt->results
  "Consume a SQL statement into a vector of results.
  Currently only returns the first string because that's all we ever need.

  Takes a mapping function `f` that will be called on each item."
  [^Stmt stmt f]
  (let [results (transient [])]
    (while (.step stmt 60)
      (let [item (.getColumnText stmt 0)]
        (conj! results (f item))))
    (.reset stmt)
    (persistent! results)))

(defn -sql-query
  "Execute the provided query. Optionally takes a function for mapping the
  response."
  ([^Conn conn stmt] (-sql-query conn stmt identity))
  ([^Conn conn [sql & args] f]
   (with-open [stmt (.prepareAndBind conn sql true (to-array args))]
     (-stmt->results stmt f))))


(defn bootstrap-connection!
  "Use the provided connection to ensure that the required tables and indices exist, as well
  as that spatialite is loaded."
  ([] (bootstrap-connection! {}))
  (^Conn
   [{:keys [db-path spatialite-path]
     :or   {db-path         Conn/MEMORY
            spatialite-path "mod_spatialite"}}]
   (doto
       (Conn/open db-path (bit-or OpenFlags/SQLITE_OPEN_CREATE
                                  OpenFlags/SQLITE_OPEN_READWRITE) nil)
       (.enableLoadExtension true)
       (.loadExtension spatialite-path nil)
       (.enableLoadExtension false)
       (-sql-query ["SELECT InitSpatialMetadata(1)"])
       (-sql-exec!
         ["CREATE TABLE IF NOT EXISTS avs (id INTEGER PRIMARY KEY NOT NULL, a VARCHAR NOT NULL, v NOT NULL, CONSTRAINT unique_av UNIQUE (a, v) ON CONFLICT IGNORE)"])
       (-sql-exec! ["CREATE INDEX IF NOT EXISTS idx_a ON avs (a)"])
       (-sql-query ["SELECT RecoverGeometryColumn('avs', 'v', 4326, 'GEOMETRY', 'XY')"])
       (-sql-query ["SELECT CreateSpatialIndex('avs', 'v')"]))))

(defrecord SpatialiteBackend [^Conn conn]
  java.io.Closeable
  (close [_] (.close conn))

  GeoBackend
  (index-av! [_ attr v]
    (-sql-exec!
      conn
      ["INSERT INTO avs (a, v) VALUES (?, ST_GeometryFromText(?, 4326))"
       (subs (str attr) 1)
       (-> v ->geo ->wkt)]))
  (evict-av! [_ attr v]
    (-sql-exec!
      conn
      ["DELETE FROM avs WHERE avs.a = ? AND avs.v = ST_GeometryFromText(?, 4326)"
       (subs (str attr) 1)
       (-> v ->geo ->wkt)]))
  (nearest-neighbors [_ attr v n]
    []
    (let [wkt (-> v ->geo ->wkt)]
      (-sql-query
        conn
        [(str "SELECT AsText(avs.v) FROM knn JOIN avs ON knn.fid = avs.id WHERE knn.f_table_name = 'avs' "
              " AND knn.ref_geometry = ST_GeometryFromText(?, 4326)"
              " AND avs.v != knn.ref_geometry"
              " AND avs.a = ?"
              " ORDER BY knn.distance ASC"
              " LIMIT ?")
         wkt (subs (str attr) 1) n]
        (comp ->geo-map wkt->geo))))
  (intersects [_ attr v]
    (let [geo (-> v ->geo)
          wkt (->wkt geo)]
      (-sql-query
        conn
        [(str "SELECT AsText(avs.v) as v FROM avs"
              " WHERE avs.id IN ("
              "  SELECT ROWID FROM SpatialIndex"
              "  WHERE f_table_name ='avs' "
              "  AND search_frame = ST_GeometryFromText(?, 4326))"
              " AND avs.v != ST_GeometryFromText(?, 4326)"
              " AND avs.a = ?")
         wkt
         wkt
         (subs (str attr) 1)]
        (comp ->geo-map wkt->geo)))))

(defn ->backend
  "Return a JTS-based Driver for crux-geo"
  {::sys/deps {:index-store :crux/index-store}
   ::sys/args {:db-path         {:doc     "The path to the local sqlite database"
                                 :default ":memory:"
                                 :spec    string?}
               :spatialite-path {:doc     "The path to mod_spatialite"
                                 :default "mod_spatialite"
                                 :spec    string?}}}
  [opts]
  (let [conn (bootstrap-connection! opts)]
    (map->SpatialiteBackend
      {:conn conn})))
