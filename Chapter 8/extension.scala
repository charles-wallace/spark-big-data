val sessions = RunGeoTime.groupByKeyAndSortValues(taxiDone, secondaryKeyFunc, RunGeoTime.split)
val daysInJanuary = 1 to 31
val sessionsByDay = daysInJanuary.map(day => sessions.filter(taxi => (taxi._2)(0).pickupTime.getDayOfMonth == day)).toList


val JanBoroughDurations: List[RDD[(Option[String], Duration)]] = daysInJanuary.map( day => 
sessionsByDay(day-1).values.flatMap(trips => {
val iter: Iterator[Seq[Trip]] = trips.sliding(2)
val viter = iter.filter(_.size == 2)
viter.map(p => boroughDuration(p(0), p(1)))
}).persist(StorageLevel.DISK_ONLY)).toList

import java.io._
val fw = new FileWriter("JanBoroughDurations.txt", true)
daysInJanuary.map(day => { fw.write("January " + day + "\n"); JanBoroughDurations(day-1).filter {
  case (b, d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds)
}).
reduceByKey((a, b) => a.merge(b)).collect().foreach(x => fw.write(x.toString + "\n"))})
fw.close()