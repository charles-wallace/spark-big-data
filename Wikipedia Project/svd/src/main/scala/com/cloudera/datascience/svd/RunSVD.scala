package com.cloudera.datascience.svd

//import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}

import breeze.linalg.norm
import breeze.math._
import breeze.numerics._
import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector, DenseVector => BDenseVector}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession,Row}
import org.apache.spark.sql.types._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
//import com.cloudera.datascience.svd.CleanFeatures 

object RunSVD {
  def main(args: Array[String]): Unit = {

val spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()
import spark.implicits._
//val termsDF = spark.read.load("/media/user/Data_Store_1/wikiQuestionTermsTitles/termsDF")
// val termsDfSample = termsDF.sample(false, .1)
val termsDF = spark.read.load("hdfs:///user/project/termsDF")
// val filtered = termsDfSample.where(size($"terms") > 1)
//val filtered = termsDF.where(size($"terms") > 1)
val filtered = termsDF.where(size($"terms") > 1)
// val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFreqs").setVocabSize(10000)
val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFreqs").setVocabSize(100000)
val vocabModel = countVectorizer.fit(filtered)
val docTermFreqs = vocabModel.transform(filtered)
val termIds = vocabModel.vocabulary
docTermFreqs.cache()
val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap
val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
val idfModel = idf.fit(docTermFreqs)
val docTermMatrix = idfModel.transform(docTermFreqs).select("title", "tfidfVec")
val termIdfs = idfModel.idf.toArray

docTermMatrix.cache()
val vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row =>
Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
}

vecRdd.cache()
val mat = new RowMatrix(vecRdd)
//val svd = mat.computeSVD(100, computeU=true)
val svd = mat.computeSVD(500, computeU=true)
//val stopWords = spark.sparkContext.textFile("stopwords.txt").collect.toSet
val stopWords = spark.sparkContext.textFile("hdfs:///user/project/stopwords.txt").collect.toSet
val  clean = new CleanFeatures(spark, stopWords)
import clean._
// val rawTrain = spark.sparkContext.textFile("FinalCleanTrain.txt").map(line => line.split(","))
val rawTrain = spark.sparkContext.textFile("hdfs:///user/project/FinalCleanTrain.txt").map(line => line.split(","))
val qpairs = rawTrain.map {case Array(id, qid1, qid2, question1, question2, is_duplicate) => Array(id, question1, question2, is_duplicate)}
val qpairsHeader = qpairs.first
val qpairsNoHeader = qpairs.filter(r => r(0) != qpairsHeader(0) && r(1) != qpairsHeader(1) && r(2) != qpairsHeader(2) && r(3) != qpairsHeader(3))

val features = qpairsNoHeader.mapPartitions(it => {
val pipeline = createNLPPipeline()
it.map { case Array(id, q1, q2, target) =>
Array(id, plainTextToLemmas(q1, pipeline).mkString(" "), 
plainTextToLemmas(q2, pipeline).mkString(" "), target)
}})

val transformFeatures = features.map{case Array(id, q1, q2, target) => (id.toDouble, q1.split(" ").toSeq, q2.split(" ").toSeq)}
val collectedFeatures = transformFeatures.collect()

val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
val idDocs: Map[String, Long] = docIds.map(_.swap)

def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
val sArr = diag.toArray
new RowMatrix(mat.rows.map { vec =>
val vecArr = vec.toArray
val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
Vectors.dense(newArr)
})
}

val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
val USzip = US.rows.zipWithUniqueId.map(_.swap)
val USBreezezip = USzip.map(r => (r._1, new BDenseVector(r._2.toArray)))
USBreezezip.cache

val U  = svd.U.rows.zipWithUniqueId.map(_.swap)
val S = new BDenseVector(svd.s.toArray)
val breezeV = new BDenseMatrix[Double](svd.V.numRows, svd.V.numCols, svd.V.toArray)
U.cache

def termsToQueryVector(terms: Seq[String]): BSparseVector[Double] = {
val indices = terms.map(idTerms(_)).toArray
val values = indices.map(termIdfs(_))
new BSparseVector[Double](indices, values, idTerms.size)
}

def topDocsForTermQuery(query: BSparseVector[Double]): Seq[(Double, Long)] = {
val termBvec = breezeV.t * query
val topWeights = USBreezezip.mapPartitions{ it =>
it.map( r => 
((r._2 :* termBvec).toArray.reduceLeft(_+_),r._1)
)
}.top(3)
topWeights
}

def getFeaturesFromLSA(docs1: Seq[(Double, Long)], docs2: Seq[(Double, Long)]) = {
// compute  uT*S for each  document row vector ui by element wise multiplication of doc vector and S vector
val utS1 = docs1.map{ case (score,id) => (new BDenseVector(U.lookup(id).head.toArray) :* S) }
val utS2 = docs2.map{ case (score,id) => (new BDenseVector(U.lookup(id).head.toArray) :* S) }
// normalize each value of uT*S
val utSnorms1 = utS1.map(uts1 => uts1/norm(uts1))
val utSnorms2 = utS2.map(uts2 => uts2/norm(uts2))
// average the resulting Vectors
val utSavg1 = ((utSnorms1.reduceLeft(_+_))*(1/(utS1.size.toDouble)))
val utSavg2 = ((utSnorms2.reduceLeft(_+_))*(1/(utS2.size.toDouble)))
// calculate cosine similarity
/** 
	Note: idk why but something quarky with spark 2.1 doesnt allow the dot product for dense
		  vectors, i implemented a simple work around for this below.
*/
val cosineSimilarity = ((utSavg1 :* utSavg2).toArray.reduceLeft(_+_))/(norm(utSavg1)*norm(utSavg2))
cosineSimilarity
}

def generateFeatures(terms1: Seq[String], terms2: Seq[String]) = {
val filTerms1 = terms1.filter(term1 => idTerms.isDefinedAt(term1))
val filTerms2 = terms2.filter(term2 => idTerms.isDefinedAt(term2))
if(!(terms1.size > 2 ||  terms2.size > 2)){
if((terms1.toSet).intersect(terms2.toSet).size > 0){
1.0
} else{0.0}
}else if((terms1.toSet == terms2.toSet)){
1.0
}else if(!(filTerms1.size > 2 || filTerms2.size > 2)){
if((filTerms1.toSet).intersect(terms2.toSet).size > 0){
1.0
} else{0.0}
}else if((filTerms1.toSet == filTerms2.toSet)){
1.0
}else if((filTerms1.toSet).intersect(filTerms2.toSet).size == 0){
0.0	
}else if(!filTerms1.isEmpty && !filTerms2.isEmpty){
val queryVec1 = termsToQueryVector(filTerms1)
val idWeights1 = topDocsForTermQuery(queryVec1)
val queryVec2 = termsToQueryVector(filTerms2)
val idWeights2 = topDocsForTermQuery(queryVec2)
val LSAfeatures = getFeaturesFromLSA(idWeights1, idWeights2)
LSAfeatures
}else {0.0}
}

val lsaFeatures =  collectedFeatures.map( f  => 
(f._1, generateFeatures(f._2, f._3)))

val lsaFeaturesDF = lsaFeatures.toSeq.toDF("id","lsacos")

lsaFeaturesDF.write.save("gs://wallacebigdatabucket/lsaFeaturesDF")

}}

