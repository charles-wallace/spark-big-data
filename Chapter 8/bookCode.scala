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
import org.apache.spark.util.StatCounter
import com.cloudera.datascience.geotime.GeoJsonProtocol._
import org.apache.spark.storage.StorageLevel


val taxiRaw = sc.textFile("hdfs:///user/cyw214/trip_data_1.csv")

val safeParse = RunGeoTime.safe(RunGeoTime.parse)

val taxiParsed = taxiRaw.map(safeParse)

val taxiGood = taxiParsed.collect({
  case t if t.isLeft => t.left.get
})

def hours(trip: Trip): Long = {
val d = new Duration(trip.pickupTime, trip.dropoffTime)
d.getStandardHours
}

val taxiClean = taxiGood.filter {
case (lic, trip) => {
val hrs = hours(trip)
0 <= hrs && hrs < 3
}
}

val geojson = scala.io.Source.fromURL(getClass.getResource("/nyc-boroughs.geojson")).mkString

val features = geojson.parseJson.convertTo[FeatureCollection]

val areaSortedFeatures = features.sortBy(f => {
val borough = f("boroughCode").convertTo[Int]
(borough, -f.geometry.area2D())
})

val bFeatures = sc.broadcast(areaSortedFeatures)

def borough(trip: Trip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.dropoffLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}

def hasZero(trip: Trip): Boolean = {
  val zero = new Point(0.0, 0.0)
  (zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
}

val taxiDone = taxiClean.filter {
case (lic, trip) => !hasZero(trip)
}

def secondaryKeyFunc(trip: Trip) = trip.pickupTime.getMillis

val sessions = RunGeoTime.groupByKeyAndSortValues(taxiDone, secondaryKeyFunc, RunGeoTime.split)

def boroughDuration(t1: Trip, t2: Trip) = {
val b = borough(t1)
val d = new Duration(
t1.dropoffTime,
t2.pickupTime)
(b, d)
}

val boroughDurations: RDD[(Option[String], Duration)] =
  sessions.values.flatMap(trips => {
    val iter: Iterator[Seq[Trip]] = trips.sliding(2)
    val viter = iter.filter(_.size == 2)
    viter.map(p => boroughDuration(p(0), p(1)))
}).persist(StorageLevel.DISK_ONLY)

