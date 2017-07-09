
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._

/** Euclidean Distance Function */
def distance(a: Vector, b: Vector) = 
	math.sqrt(a.toArray.zip(b.toArray).
		map(p => p._1 - p._2).map(d => d * d).sum)

/** Calculate distance of point from cluster centroid */
def distToCentroid(datum: Vector, model: KMeansModel) = {
	val cluster = model.predict(datum)
	val centroid = model.clusterCenters(cluster)
	distance(centroid, datum)
}

/** Scoring a cluster */
def clusteringScore(data: RDD[Vector], k: Int) = {
	val kmeans = new KMeans()
	kmeans.setK(k)
	val model = kmeans.run(data)
	data.map(datum => distToCentroid(datum, model)).mean()
}

val rawData = sc.textFile("kddcup.data")

val labelsAndData = rawData.map { line => 
	// to buffer creates a buffer (immutable list)
	val buffer = line.split(',').toBuffer
	buffer.remove(1, 3)
	val label = buffer.remove(buffer.length-1)
	val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
	(label,vector)
}

val data = labelsAndData.values.cache()

/**
	######	   ##   ######  ########  ###### 
	##	  #   ####  ##	  #    ##       ##
	######	 ## ##  ###### 	   ##       ##
	##		####### ###  ##	   ##	    ##
	##	   ##	 ## ##   ##	   ##     ######
*/

/** 1) Exploring values of K beyond 100 */

/** Examining results between 100 and 250 */
//(100 to 250 by 10).par.map(k => (k, clusteringScore(data, k))).toList.foreach(println)

/**
	Results:
		(100,138.31456210138407)                                                        
		(110,115.64724609658472)
		(120,113.51212973717665)
		(130,99.71736960409376)
		(140,99.99945461538822)
		(150,84.33491254081812)
		(160,70.18091110269312)
		(170,65.8555258428412)
		(180,76.9326257371835)
		(190,61.08026414854447)
		(200,57.59447180920991)
		(210,51.371103104596365)
		(220,55.57522285528236)
		(230,52.2306951299304)
		(240,44.94054761143083)
		(250,44.302807005494174)
*/

val dataAsArray = data.map(_.toArray)
val numCols = dataAsArray.first().length
val n = dataAsArray.count()
val sums = dataAsArray.reduce((a,b) => a.zip(b).map(t => t._1 + t._2))
val sumSquares = dataAsArray.fold(new Array[Double](numCols))((a,b) => a.zip(b).map(t => t._1 + t._2 * t._2))
val stdevs = sumSquares.zip(sums).map {case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n}
val means = sums.map(_ / n)

/** Function to Normalize the dataset */
def normalize(datum: Vector) = {
	val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
		(value, mean, stdev) =>
			if (stdev <= 0) (value - mean) else (value - mean) / stdev
	)
	Vectors.dense(normalizedArray)
}

/** Normalizing the Data */
val normalizedData = data.map(normalize).cache()

/**
	######	   ##   ######  ########  ###########
	##	  #   ####  ##	  #    ##       ##  ##
	######	 ## ##  ###### 	   ##       ##  ##
	##		####### ###  ##	   ##	    ##  ##
	##	   ##	 ## ##   ##	   ##     ###########
*/

/** Exploring values of K beyond 100 for normalized features */
(100 to 250 by 10).par.map(k => (k, clusteringScore(normalizedData, k))).toList.foreach(println)
data.unpersist()

/**
	Results:
		(100,0.0017550743103169353)                                                     
		(110,0.0015815777376560719)
		(120,0.0014211726670495745)
		(130,0.0013102217522098637)
		(140,0.0011761491761769728)
		(150,0.0011100259142761575)
		(160,0.0011703375216085047)
		(170,9.828395510082003E-4)
		(180,0.0010144922410875328)
		(190,9.313810125543788E-4)
		(200,9.021376850391825E-4)
		(210,8.908134008278499E-4)
		(220,7.493488016871751E-4)
		(230,7.996499726900795E-4)
		(240,7.25682289815949E-4)
		(250,7.208963473968953E-4)
*/

