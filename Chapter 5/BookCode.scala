import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._


/** Create Spark Context object from kddcup data */
val rawData = sc.textFile("kddcup.data")

/** Figuring out what and how many labels are present in the dataset*/
//rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)


/** 
	Storing each feature vector and assigned label in a tuple with categorical
	attributes removed and the remaining numeric features converted to 
	doubles. 
*/
val labelsAndData = rawData.map { line => 
	// to buffer creates a buffer (immutable list)
	val buffer = line.split(',').toBuffer
	buffer.remove(1, 3)
	val label = buffer.remove(buffer.length-1)
	val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
	(label,vector)
}

val data = labelsAndData.values.cache()

/** Cluster Data with the Kmeans algorithm. */
val kmeans = new KMeans()
val model = kmeans.run(data)

/** Print Centroid */
model.clusterCenters.foreach(println)

/** 
	Assign each datapoint to a cluster and count occurances of 
	the cluster label pairs using the model
*/

val clusterLabelCount = labelsAndData.map { case (label,datum) => 
	val cluster = model.predict(datum)
	(cluster,label)
}.countByValue

// NOTE: f"$cluster%1s$label%18s$count%8s format string interpolates and formats variables
clusterLabelCount.toSeq.sorted.foreach {
	case ((cluster,label),count) =>
	println(f"$cluster%1s$label%18s$count%8s")
}

/** Euclidean Distance Function */
def distance(a: Vector, b: Vector) = 
	math.sqrt(a.toArray.zip(b.toArray).
		map(p => p._1 - p._2).map(d => d * d).sum)

/** 
	Function using Euclidean Distance function to compute distance to 
	closest Cluster Centroid.
*/
def distToCentroid(datum: Vector, model: KMeansModel) = {
	val cluster = model.predict(datum)
	val centroid = model.clusterCenters(cluster)
	distance(centroid, datum)
}

/** Measuring the average distance of points to cluster centroids */
def clusteringScore(data: RDD[Vector], k: Int) = {
	val kmeans = new KMeans()
	kmeans.setK(k)
	val model = kmeans.run(data)
	data.map(datum => distToCentroid(datum, model)).mean()
}

/** Evaluate the success of K values from 5 to 40 in increments of 5 */
(5 to 40 by 5).map(k => (k, clusteringScore(data, k))).foreach(println)

/** Evalute the success of K in parrallell with longer iterations and epsilon */
(30 to 100 by 10).par.map(k => (k, clusteringScore(data, k))).toList.foreach(println)

/** 
	Computing Standard scores jointly with count, sum, and sum-of-squares
	using reduce operations to add entire arrays at once, then fold to 
	accumulate sums of squares from an array of zeros
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

/** Testing Normalized Data set at various Values of K*/
(60 to 120 by 10).par.map(k => (k, clusteringScore(normalizedData, k))).toList.foreach(println)


/** Create entropy function */
def entropy(counts: Iterable[Int]) = {
	val values = counts.filter(_ > 0)
	val n: Double = values.sum
	values.map { v =>
		val p = v / n
		-p * math.log(p)
	}.sum
}

/** Taken from book code to create normalizedLabelsAndData */
def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
        new Array[Double](numCols)
      )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
        (a, b) => a.zip(b).map(t => t._1 + t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
}

/** Function to score Success of Clustering with Entropy*/
  def clusteringScoreEntropy(normalizedLabelsAndData: RDD[(String,Vector)], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(normalizedLabelsAndData.values)

    // Predict cluster for each datum
    val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)

    // Swap keys / values
    val clustersAndLabels = labelsAndClusters.map(_.swap)

    // Extract collections of labels, per cluster
    val labelsInCluster = clustersAndLabels.groupByKey().values

    // Count labels in collections
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))

    // Average entropy weighted by cluster size
    val n = normalizedLabelsAndData.count()

    labelCounts.map(m => m.sum * entropy(m)).sum / n
}

/** Use Categorical Data*/
def buildCategoricalAndLabelFunction(rawData: RDD[String]): (String => (String,Vector)) = {
    val splitData = rawData.map(_.split(','))
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    (line: String) => {
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures)
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1, newProtocolFeatures)

      (label, Vectors.dense(vector.toArray))
    }
  }



/** Use categorical variables by converting to binary features */
val parseFunction = buildCategoricalAndLabelFunction(rawData)
/** store the values */
val data = rawData.map(parseFunction).values
/** getting labels and normalized data to test entropy */
val normalizedLabelsAndData = labelsAndData.mapValues(buildNormalizationFunction(labelsAndData.values)).cache()
/** Build The Model */
val model = kmeans.run(normalizedLabelsAndData.values)
/** Predict cluster for each datum */
val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)
/** Swap keys/values */
val clustersAndLabels = labelsAndClusters.map(_.swap)
/** Extract collections of labels, per cluster */
val labelsInCluster = clustersAndLabels.groupByKey().values
/** Count labels in collections */
val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))
val n = normalizedLabelsAndData.count()
/** Average entropy weighted by cluster size */
labelCounts.map(m => m.sum * entropy(m)).sum / n)

/** a par.map can be used to test successive values of k */
(100 to 200 by 10).par.map(k => (k, clusteringScoreEntropy(normalizedLabelsAndData, k))).toList.foreach(println)
