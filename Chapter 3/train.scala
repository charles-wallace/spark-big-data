import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation._
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

  
/**
  The following functions were taken from the source code accompanying the book.
*/
  def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def areaUnderCurve(
      positiveData: RDD[Rating],
      bAllItemIDs: Broadcast[Array[Int]],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }


/**
  The Following Code is an Extension to the code provided in the book.
*/

case class Parameters(rank: Int, iterations: Int, lambda: Double, alpha: Double)
case class ModelData(param: Parameters, auc: Double, percentTrain: Double, percentCV: Double)
def printParameters(param: Parameters){ 
  print(param.rank.toInt + " ")
}

/** An ALSmodel object holds a number of models in an array specified by the size parameter */
class ALSTester(size: Int) extends Serializable { 

	import org.apache.spark.rdd.RDD
	import org.apache.spark.mllib.recommendation._
	import org.apache.spark.broadcast.Broadcast
	import scala.collection.Map
  import scala.collection.mutable
  // Simple Array to store 
	val models: Array[MatrixFactorizationModel] = new Array[MatrixFactorizationModel](size)
  val testResults = mutable.Map[Int, ModelData]()

	var allData: RDD[Rating] = sc.parallelize(Seq.empty[Rating])
  var trainData: RDD[Rating] = sc.parallelize(Seq.empty[Rating])
	var cvData: RDD[Rating] = sc.parallelize(Seq.empty[Rating])
	var crossValidateBool: Boolean = false
	var bAllItemIDs: Broadcast[Array[Int]] = sc.broadcast(Array())
	var modelCount: Int = 0
	var testCount: Int = 0
  var percentTrain: Double = 1.0
  var percentCV: Double = 0.0

	def pushTest(rank: Int, iterations: Int, lambda: Double, alpha: Double) = {
		if (allData.take(1) != Array() || ((trainData.take(1) != Array()) && (cvData.take(1) != Array()))){
			if (modelCount < size) {	
				var auc: Double = Double.NaN
        if (crossValidateBool == false) {
          //val auc = Double.NaN
					models(modelCount) = ALS.trainImplicit(allData, rank, iterations, lambda, alpha)
          testResults += (testCount -> ModelData(Parameters(rank, iterations, lambda, alpha), auc, 1.0, 0.0))
				} else {
					models(modelCount) = ALS.trainImplicit(trainData, rank, iterations, lambda, alpha)
				  auc = areaUnderCurve(cvData, bAllItemIDs, models(modelCount).predict)
          testResults += (testCount -> ModelData(Parameters(rank, iterations, lambda, alpha), auc, percentTrain, percentCV))
				}
        
				modelCount += 1
        testCount += 1
				this
			} else {
				println("ERROR: Model Stack Full. Please Pop or Empty Model Stack.")
				None
			}
		} else {
			println("ERROR: Data Must Be Loaded to ALSModelTester to Train.")
			None
		}
	}

  def popTest() {
    unpersist(models(modelCount-1))
    modelCount -= 1
  }

	def loadData(rawUserArtistData: RDD[String], rawArtistAlias: RDD[String]) {
    /** Maps artist IDs that may be misspelled or nonstandard to the ID of the artist’s canonical name.*/
		val artistAlias: scala.collection.Map[Int,Int]  = rawArtistAlias.flatMap { line =>
			val tokens = line.split('\t')
			if (tokens(0).isEmpty) {
				None
			} else {
				Some((tokens(0).toInt, tokens(1).toInt))
			}
		}.collectAsMap()
    /** broadcast artistAlias to make spark send and hold in memory one copy for each executer */
		val bArtistAlias: org.apache.spark.broadcast.Broadcast[scala.collection.Map[Int,Int]] = sc.broadcast(artistAlias)
		allData = buildRatings(rawUserArtistData, bArtistAlias)
	}

	def CV(perTrain: Double, perCV: Double) = {
    if(crossValidateBool == true){
      bAllItemIDs.destroy()
    }
    percentTrain = perTrain
    percentCV = perCV
    val split = allData.randomSplit(Array(percentTrain, percentCV))
		trainData = split(0)
    cvData = split(1)
    trainData.cache()
		cvData.cache()
		bAllItemIDs = sc.broadcast(allData.map(_.product).distinct().collect())
		crossValidateBool = true
	}

  def CVOFF() = {
    crossValidateBool = false
  }

  def printResults(){
    testResults.keys.foreach(i => println("Test: " + i + " " + testResults(i).param + " %Train(" + testResults(i).percentTrain + ") %CV(" + testResults(i).percentCV + ")" + " AUC: " + testResults(i).auc))
  }


}




