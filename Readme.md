# crux-geo

Geospatial indexing for [juxt/crux](https://github.com/juxt/crux).


## Configuration

To use `crux-geo` simply add the `:teknql.crux-geo/geo-store` to your crux configuration.

```clojure
(crux/start-node {:teknql.crux-geo/geo-store {}})
```

## Writing Data

To express geospatial `crux-geo` expects maps with data similar to that as found
in the [GeoJSON Specification](https://datatracker.ietf.org/doc/html/rfc7946).

Specifically maps should have two keys `:geometry/type` and
`:geometry/coordinates`. The following types are supported, using the same
coordinate patterns as seen in GeoJSON:

- `:geometry.type/point`
- `:geometry.type/multi-point`
- `:geometry.type/line-string`
- `:geometry.type/polygon`
- `:geometry.type/multi-polygon`

Below is an example of storing a city with a lng/lat point:


``` clojure
(crux/submit-tx
  [:crux.tx/put
   {:crux.db/id :city.id/new-york
    :city/location
    {:geometry/type        :geometry.type/point
     :geometry/coordinates [-74.0060 40.7128]}}])
```

## Querying Data

crux-geo provides a range of useful query operations. For a potentially more
complete reference, check the tests folder.

Consider a database with the following data:

``` clojure
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
   :geometry/coordinates [-87.6298 41.8781]}}]
```


### Nearest Geometries

``` clojure

(crux/q
  (crux/db node)
  '{:find  [?nearest]
    :where [[?ny :crux.db/id :city.id/new-york]
            [?ny :city/location ?loc]
            [(geo-nearest :city/location ?loc) [[?nearest]]]]})
=> #{[:city.id/boston]}
```

The second argument in the resulting tuple can be used to immediately grab the
matched location without needing to query it from the attribute:

``` clojure

(crux/q
  (crux/db node)
  '{:find  [?nearest ?nearest-loc]
    :where [[?ny :crux.db/id :city.id/new-york]
            [?ny :city/location ?loc]
            [(geo-nearest :city/location ?loc) [[?nearest ?nearest-loc]]]]})
=>
#{[:city.id/boston
  {:geometry/coordinates [-71.0589 42.3601],
   :geometry/type        :geometry.type/point}]}
```

#### Getting `N` nearest

You may optionally provide a number to `geo-nearest` to query for the `n`
nearest items:

``` clojure
(crux/q
  (crux/db node)
  '{:find     [?nearest]
    :order-by [[?nearest :asc]]
    :where    [[?ny :crux.db/id :city.id/new-york]
               [?ny :city/location ?loc]
               [(geo-nearest :city/location ?loc 2) [[?nearest]]]]})
=> [[:city.id/boston] [:city.id/chicago]]
```

### Intersecting Geometries

The `geo-intersects` predicate may be used to filter for values that intersect
with each other. Here is an example that uses a bounding box
(`:geometry.type/polygon`) to search for cities contained within:

``` clojure
(crux/q
  (crux/db node)
  '{:find  [?city]
    :in    [bounding-box]
    :where [[(geo-intersects :city/location bounding-box) [[?city]]]]}
  {:geometry/type :geometry.type/polygon
   :geometry/coordinates
   [[[-74.04853820800781 40.6920928987952]
     [-73.92013549804688 40.6920928987952]
     [-73.92013549804688 40.78834006798032]
     [-74.04853820800781 40.78834006798032]
     [-74.04853820800781 40.6920928987952]]]})
=> #{[:city.id/new-york]}
```

Similar to `geo-nearest`, `geo-intersects` can also return the value of the
matched attribute as an additional argument to the binding tuple.

## How It Works and Roadmap

crux-geo is built on top of [JTS](https://github.com/locationtech/jts).
Currently it stores all values in in-memory STRtrees partitioned by attribute.
These trees are automatically re-built using the index stores at node startup.

Ultimately we may explore using on-disk structures directly, as well as allow
for configuration of different backends.

