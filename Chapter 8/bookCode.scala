/** 
	The Spark MLlib abstraction for a feature vector, "LabeledPoint", is a Spark MLlib Vector of features, and a target value.
*/

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._


val rawData = sc.textFile("covtype.data")
val data = rawData.map { line =>
	val values = line.split(',').map(_.toDouble) 
	// init returns all but last value; target is last column
	val featureVector = Vectors.dense(values.init) 
	// DecisionTree needs labels starting at 0; subtract 1
	val label = values.last - 1 
	LabeledPoint(label, featureVector)
}

/** Data split for training, cross validation, and testing */
val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
trainData.cache()
cvData.cache()
testData.cache()

/** 
	building a DecisionTreeModel on the training set, with some default arguments, 
	and computing metrics about the resulting model using the CV set
*/

// computes standard metrics to measure the quality of the predictions from a classifier
def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
	val predictionsAndLabels = data.map(example => 
		(model.predict(example.features), example.label)
	)
	new MulticlassMetrics(predictionsAndLabels) 
}

/** Commenting out training */
val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)

val metrics = getMetrics(model, cvData)

/** printing confusion matrix */
metrics.confusionMatrix
/** printing accuracy */
metrics.precision

/** 
	printing out precision and recall
*/
(0 until 7).map(
cat => (metrics.precision(cat), metrics.recall(cat))
).foreach(println)

/**
	summing products of probabilities for precision pg 70
*/
def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = { 
	// count (category, count) in data
	val countsByCategory = data.map(_.label).countByValue()
	// ordercounts by category and extract counts
	val counts = countsByCategory.toArray.sortBy(_._1).map(_._2) 
	counts.map(_.toDouble / counts.sum)
}

val trainPriorProbabilities = classProbabilities(trainData) 
val cvPriorProbabilities = classProbabilities(cvData) 
// pair probability in training, CV set and sum products
trainPriorProbabilities.zip(cvPriorProbabilities).map {
	case (trainProb, cvProb) => trainProb * cvProb 
}.sum

val evaluations = for (impurity <- Array("gini", "entropy");
	 depth <- Array(1, 20);
	 bins <- Array(10, 300)
	)
	yield {
		val model = DecisionTree.trainClassifier(
		trainData, 7, Map[Int,Int](), impurity, depth, bins)
		val predictionsAndLabels = cvData.map(example =>
			(model.predict(example.features), example.label)
		)
		val accuracy =
			new MulticlassMetrics(predictionsAndLabels).precision
          	((impurity, depth, bins), accuracy)
 }
 // sort by decending accuracy
 evaluations.sortBy(_._2).reverse.foreach(println)
