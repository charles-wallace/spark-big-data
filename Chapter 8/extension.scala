import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.util.StatCounter
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

def getMetrics(model: NaiveBayesModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
	val predictionsAndLabels = data.map(example => 
		(model.predict(example.features), example.label)
	)
	new MulticlassMetrics(predictionsAndLabels) 
}
/**
	attempting naieve bayes
*/
/** Call to get the full data set */
val rawData = sc.textFile("covtype.data")
// exploring the data
// val values  = rawData.map(line => line.split(',').map(_.toDouble))

// getting stats for each of the first 10 lines found negative values at fourth index

/**
val stats = (0 until 10).map(i => {values.map(x => x(i)).stats()})
stats.foreach(println)
*/

// found negative values at index value four this produces terrible precision and recall
/**
val bayesData = rawData.map { line =>
	val fullValues = line.split(',').map(_.toDouble)
	val partialValues = Array(fullValues.slice(0,4),fullValues.slice(5,fullValues.length)).flatten
	val featureVector = Vectors.dense(partialValues.init) 
	val label = partialValues.last - 1 
	LabeledPoint(label, featureVector)
}
*/
/**
val bayesData2 = rawData.map { line =>
	val fullValues = line.split(',').map(_.toDouble)
	val wilderness = fullValues.slice(10, 14).indexOf(1.0).toDouble
	val soil = fullValues.slice(14, 54).indexOf(1.0).toDouble 
	val partialValues = Array(fullValues.slice(0,4),fullValues.slice(5,10)).flatten
	val featureVector = Vectors.dense(partialValues :+ wilderness :+ soil)  
	val label = fullValues.last - 1 
	LabeledPoint(label, featureVector)
}
*/

val bayesData3 = rawData.map { line =>
	val values = line.split(',').map(_.toDouble).slice(10, 55) 
	// init returns all but last value; target is last column
	val featureVector = Vectors.dense(values.init) 
	// DecisionTree needs labels starting at 0; subtract 1
	val label = values.last - 1 
	LabeledPoint(label, featureVector)
}


/**
val Array(trainData, cvData, testData) = bayesData.randomSplit(Array(0.8, 0.1, 0.1))
trainData.cache()
cvData.cache()
testData.cache()
*/

// train the model with naive bayes
/**
	val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "multinomial")
*/
// get metrics
/**
val metrics = getMetrics(model, cvData)
*/
