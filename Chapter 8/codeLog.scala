/** 
	Starting on hadoop cluster:
	spark-shell --master yarn --deploy-mode client --jars /common/aasCode/aas-1st-edition/ch08-geotime/target/ch08-geotime-1.0.2-jar-with-dependencies.jar
*/

import com.github.nscala_time.time.Imports._
import java.text.SimpleDateFormat
import com.esri.core.geometry.Geometry
import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry.SpatialReference
import com.esri.core.geometry.Point
import org.joda.time.{DateTime, Duration}
import com.cloudera.datascience.geotime._
import spray.json._
import spray.json.JsValue
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter
import com.cloudera.datascience.geotime.GeoJsonProtocol._


/**
for cluster:
val taxiRaw = sc.textFile("hdfs:///user/cyw214/trip_data_1.csv")

for local:
val taxiRaw = sc.textFile("taxidata")

*/
val safeParse = RunGeoTime.safe(RunGeoTime.parse)
val taxiParsed = taxiRaw.map(safeParse)
taxiParsed.cache()

val taxiGood = taxiParsed.collect({
  case t if t.isLeft => t.left.get
})
taxiGood.cache()


def hours(trip: Trip): Long = {
val d = new Duration(trip.pickupTime, trip.dropoffTime)
d.getStandardHours
}

taxiParsed.unpersist()

val taxiClean = taxiGood.filter {
case (lic, trip) => {
val hrs = hours(trip)
0 <= hrs && hrs < 3
}
}

/**
	***********************
	* Geospatial Analysis * 
	***********************
*/

/** Perform Borough Analysis to Limit Out of Bound Spatial Taxi Data*/
val geojson = scala.io.Source.fromURL(getClass.getResource("/nyc-boroughs.geojson")).mkString

/** Convert Json data to FeatureCollection*/
val features = geojson.parseJson.convertTo[FeatureCollection]

/** Sortting features by borroughCode and 2D area, placing larger regions before smaller regions */
val areaSortedFeatures = features.sortBy(f => {
val borough = f("boroughCode").convertTo[Int]
(borough, -f.geometry.area2D())
})

/** Broadcast sorted features */
val bFeatures = sc.broadcast(areaSortedFeatures)

/** function to determine which burrough a trip ended */
def borough(trip: Trip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.dropoffLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}

/** 
	Creating Histogram of trips by burrough:
	(Some(Staten Island),3338)
	(Some(Queens),672135)
	(Some(Manhattan),12978954)
	(Some(Bronx),67421)
	(Some(Brooklyn),715235)
	(None,338937)

*/
taxiClean.values.
map(borough).
countByValue().
foreach(println)

/** Function to filter zero values for trip*/
def hasZero(trip: Trip): Boolean = {
  val zero = new Point(0.0, 0.0)
  (zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
}

/** Filtering zero values for trip */
val taxiDone = taxiClean.filter {
case (lic, trip) => !hasZero(trip)
}.cache()

taxiGood.unpersist()

/** 
	Building Histogram Again with zero valued trips removed:
	(Some(Staten Island),3333)
	(Some(Queens),670996)
	(Some(Manhattan),12973001)
	(Some(Bronx),67333)
	(Some(Brooklyn),714775)
	(None,65353)

*/
taxiDone.values.
map(borough).
countByValue().
foreach(println)

/**
	***************************
	* Sessionization in Spark * 
	***************************
*/

/** Secondary Key to sort on */
def secondaryKeyFunc(trip: Trip) = trip.pickupTime.getMillis

/** 
	Establishing split criteria, setting duration  beyond which longer pickups will 
    be considered seperate shift. and intermediate a driving break
*/
def split(t1: Trip, t2: Trip): Boolean = {
val p1 = t1.pickupTime
val p2 = t2.pickupTime
val d = new Duration(p1, p2)
d.getStandardHours >= 4
}
/** group and sort data*/
val sessions = RunGeoTime.groupByKeyAndSortValues(taxiDone, secondaryKeyFunc, RunGeoTime.split)

/** 
    takes two instances of the Trip class and computes both the borough of the first trip and the 
    Duration between the dropoff time of the first trip and the pickup time of the second 
*/
def boroughDuration(t1: Trip, t2: Trip) = {
val b = borough(t1)
val d = new Duration(
t1.dropoffTime,
t2.pickupTime)
(b, d)
}

/** apply boroughDuration function to all sequential pairs of trips inside of sessions RDD */
val boroughDurations: RDD[(Option[String], Duration)] =
  sessions.values.flatMap(trips => {
    val iter: Iterator[Seq[Trip]] = trips.sliding(2)
    val viter = iter.filter(_.size == 2)
    viter.map(p => boroughDuration(p(0), p(1)))
}).persist(StorageLevel.MEMORY_AND_DISK_SER)

/**  Checking non-negative values */
boroughDurations.values.map(_.getStandardHours).countByValue().toList.sorted.foreach(println)

taxiDone.unpersist()

/** Statistics on wait time for drivers by burrough:
(Some(Staten Island),(count: 2612, mean: 2612.235835, stdev: 2186.286857, max: 13740.000000, min: 0.000000))
(Some(Bronx),(count: 56951, mean: 1945.791452, stdev: 1617.687601, max: 14116.000000, min: 0.000000))
(Some(Brooklyn),(count: 626231, mean: 1348.675465, stdev: 1565.119331, max: 14355.000000, min: 0.000000))
(Some(Queens),(count: 557827, mean: 2338.242654, stdev: 2120.982962, max: 14378.000000, min: 0.000000))
(Some(Manhattan),(count: 12505455, mean: 622.580038, stdev: 1022.340271, max: 14310.000000, min: 0.000000))
(None,(count: 57685, mean: 1922.096559, stdev: 1903.766133, max: 14280.000000, min: 0.000000))

*/

boroughDurations.filter {
  case (b, d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds)
}).
reduceByKey((a, b) => a.merge(b)).collect().foreach(println)


