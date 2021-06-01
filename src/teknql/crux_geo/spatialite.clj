(ns teknql.crux-geo.spatialite
  "Spatialite backend for crux-geo"
  (:require [next.jdbc :as jdbc]
            [teknql.crux-geo :refer [GeoBackend]]
            [teknql.crux-geo.jts :refer [->geo ->geo-map]]
            [crux.system :as sys])
  (:import [org.locationtech.jts.io WKTWriter WKTReader]
           [org.locationtech.jts.geom Geometry]))

(def ^:private writer
  "Static writer for WKT"
  (WKTWriter.))

(def ^:private reader
  "Static reader for WKT"
  (WKTReader.))

(defn ->wkt
  "Convert a Geometry object into WKT representation"
  [^Geometry geo]
  (.write ^WKTWriter writer geo))

(defn wkt->geo
  [^String wkt-text]
  (.read ^WKTReader reader wkt-text))

(defrecord SpatialiteBackend [conn]
  java.io.Closeable
  (close [_] (.close conn))

  GeoBackend
  (index-av! [_ attr v]
    (jdbc/execute-one!
      conn
      ["INSERT INTO avs (a, v) VALUES (?, ST_GeometryFromText(?, 4326))"
       (subs (str attr) 1)
       (-> v ->geo ->wkt)]))
  (evict-av! [_ attr v]
    (jdbc/execute-one!
      conn
      ["DELETE FROM avs WHERE avs.a = ? AND avs.v = ST_GeometryFromText(?, 4326)"
       (subs (str attr) 1)
       (->wkt v)]))
  (nearest-neighbors [_ attr v n]
    (let [wkt (-> v ->geo ->wkt)]
      (->> (jdbc/execute!
             conn
             [(str "SELECT AsText(avs.v) as v FROM knn JOIN avs ON knn.fid = avs.id WHERE f_table_name = 'avs' "
                   " AND ref_geometry = ST_GeometryFromText(?, 4326)"
                   " AND avs.v != ST_GeometryFromText(?, 4326)"
                   " AND avs.a = ?"
                   " AND max_items = ?")
              wkt
              wkt
              (subs (str attr) 1)
              n])
           (map (comp ->geo-map wkt->geo :v)))))
  (intersects [_ attr v]
    (let [wkt (-> v ->geo ->wkt)]
      (->> (jdbc/execute!
             conn
             [(str "SELECT AsText(avs.v) as v FROM avs"
                   " WHERE Intersects(avs.v, ST_GeometryFromText(?, 4326)) = 1"
                   " AND avs.v != ST_GeometryFromText(?, 4326)"
                   " AND avs.a = ?")
              wkt
              wkt
              (subs (str attr) 1)])
           (map (comp ->geo-map wkt->geo :v))))))

(defn bootstrap-connection!
  "Use the provided connection to ensure that the required tables and indices exist, as well
  as that spatialite is loaded."
  [{:keys [db-dir spatialite-path]}]
  (let [conn (jdbc/get-connection {:dbtype                 "sqlite"
                                   :dbname                 db-dir
                                   "enable_load_extension" true})]
    (doto conn
      (jdbc/execute-one!
        ["SELECT load_extension(?)" spatialite-path])
      (jdbc/execute-one!
        ["SELECT InitSpatialMetadata(1)"])
      (jdbc/execute-one!
        ["CREATE TABLE IF NOT EXISTS avs (id INTEGER PRIMARY KEY NOT NULL, a VARCHAR NOT NULL, v NOT NULL, CONSTRAINT unique_av UNIQUE (a, v) ON CONFLICT IGNORE)"])
      (jdbc/execute-one!
        ["CREATE INDEX idx_a ON avs (a)"])
      (jdbc/execute-one!
        ["SELECT RecoverGeometryColumn('avs', 'v', 4326, 'GEOMETRY', 'XY')"])
      (jdbc/execute-one!
        ["SELECT CreateSpatialIndex('avs', 'v')"])
      #_(jdbc/execute-one!
          ["CREATE VIRTUAL TABLE knn USING VirtualKNN()"]))))

(defn ->backend
  "Return a JTS-based Driver for crux-geo"
  {::sys/deps {:index-store :crux/index-store}
   ::sys/args {:db-dir          {:doc     "The path to the local sqlite database"
                                 :default ":memory:"
                                 :spec    string?}
               :spatialite-path {:doc     "The path to mod_spatialite"
                                 :default "mod_spatialite"
                                 :spec    string?}}}
  [opts]
  (-> opts
      (bootstrap-connection!)
      (->SpatialiteBackend)))
(comment
  (with-open [be (->backend {:db-dir ":memory:" :spatialite-path "/nix/store/czlsm3jgr5s96l8i0hzknpqk3ywppsrp-libspatialite-4.3.0a/lib/mod_spatialite"})]))
