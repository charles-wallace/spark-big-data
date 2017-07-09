package com.cloudera.datascience.filter

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object RunFilter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()
     import spark.implicits._
    val allQuestions = spark.sparkContext.textFile("hdfs:///user/project/AllQuestionLemmas.txt").map(_.split(" ").toSet).reduce(_|_)
    def removeBrackets(rowString: String): String = {
        val rowSize = rowString.length
        val cleanString = rowString.substring(1, rowString.length-1)
        cleanString
    }
    val wikiTitles = spark.read.load("hdfs:///user/project/filteredTitles")
    val titleSet = wikiTitles.select("value").map(row => removeBrackets(row.toString())).rdd.collect.toSet

    val stopWords = spark.sparkContext.textFile("hdfs:///user/project/stopwords.txt").collect.toSet

    val filterTitles = new FilterTitles(spark, allQuestions, stopWords, titleSet)
    import filterTitles._
    val docTexts: Dataset[(String, String)] = parseWikipediaDump("hdfs:///user/project/Wikipedia")
    documentTermMatrix(docTexts)
  }
}