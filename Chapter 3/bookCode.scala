import org.apache.spark.rdd.RDD
import scala.collection.Map._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val rawUserArtistData: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/user_artist_data.txt")
val rawArtistData: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/artist_data.txt")
val rawArtistAlias: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/artist_alias.txt")

/** 
	Artist names corresponding to the opaque numeric IDs.
 */
 
val artistByID: org.apache.spark.rdd.RDD[(Int, String)] = rawArtistData.flatMap { line => 
		val (id, name) = line.span(_ != '\t')
		if (name.isEmpty) {
			None
		}else{ 
			try {
				Some((id.toInt, name.trim)) 
			} catch {
				case e: NumberFormatException => None 
			}
		} 
	}

/** 
	Maps artist IDs that may be misspelled or nonstandard 
	to the ID of the artist’s canonical name.
 */
val artistAlias: scala.collection.Map[Int,Int]  = rawArtistAlias.flatMap { line =>
		val tokens = line.split('\t')
		if (tokens(0).isEmpty) {
			None
		} else {
			Some((tokens(0).toInt, tokens(1).toInt))
		}
	}.collectAsMap()
/**
	broadcast artistAlias to make spark send and hold in memory one copy
	for each executer
*/
val bArtistAlias: org.apache.spark.broadcast.Broadcast[scala.collection.Map[Int,Int]] = sc.broadcast(artistAlias)

/**
// simple training data without cross validation

val trainData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating]  = rawUserArtistData.map { line => val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID =
    bArtistAlias.value.getOrElse(artistID, artistID)
    Rating(userID, finalArtistID, count)
    }.cache()
*/

/** Cross Validation using buildRatings and areaUnderCurve from the code accompanying the book */

/**
##########################################################################################
# The following functions were taken from the source code accompanying the book.         #
# to complete the excersises from pg 51 on.												 #
##########################################################################################
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
##########################
#	End Companion Code   #
##########################
*/


/** from page 51 and 52, code to compute AUC*/


val allData = buildRatings(rawUserArtistData, bArtistAlias)
val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1)) 
trainData.cache()
cvData.cache()
val allData = buildRatings(rawUserArtistData, bArtistAlias)
val allItemIDs = allData.map(_.product).distinct().collect() 
val bAllItemIDs = sc.broadcast(allItemIDs)

val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)


val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)


/** 
	Function for predicting most listened. This partially applied function
	provides a bench mark to measure the relative effectiveness of the ALS
	model by returning a function that predicts users tast based on the most-played
	music without any personalization.
*/


def predictMostListened(
	sc: SparkContext,
	train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
	val bListenCount = sc.broadcast( train.map(r => (r.product, r.rating)).
	reduceByKey(_ + _).collectAsMap() )
	allData.map { case (user, product) => Rating(
          user,
          product,
          bListenCount.value.getOrElse(product, 0.0)
		) 
	}
}

val aucMostListened = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))


/**
	#############################################
	## 			Making Recommendations         ##
	#############################################
*/

val someUsers = allData.map(_.user).distinct().take(100)
val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5)) 
someRecommendations.map(recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ") ).foreach(println)