
package com.cloudera.datascience.getArticles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
/** 
	Filters Wikipedia Article Titles From List Collected Using The Filter
	Code By Searching For The Presence Of Each Title Within Each Individual 
	Question. Each Time A Question Is Found To Contain A Title It Is Removed.

*/
object RunGetArticles {
def main(args: Array[String]): Unit = {

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
val titleStrings = spark.sparkContext.textFile("hdfs:///user/project/titleStrings").toDS
val AllQuestionLemmas = spark.sparkContext.textFile("hdfs:///user/project/AllQuestionLemmas.txt")
val qBuffer: ArrayBuffer[String] = new ArrayBuffer()
val collectedQuestions = AllQuestionLemmas.collect
collectedQuestions.map(question => qBuffer.append(question))

val filteredStrings = titleStrings.filter( title => {
val qIndexBuffer: ArrayBuffer[Int] = new ArrayBuffer()
var i = 0
var found=0
while(i < qBuffer.size){
	if(qBuffer(i).contains(title)){
		qIndexBuffer.append(i)
		found = 1
	}
	i+=1
}//endwhile
	if(found == 1){ 
		for(qIndex <- (qIndexBuffer.size - 1) to 0 by -1){
			qBuffer.remove(qIndexBuffer(qIndex))
		}
		true
	}
	else false
})

filteredStrings.write.save("hdfs:///user/project/filteredTitles")

}}

