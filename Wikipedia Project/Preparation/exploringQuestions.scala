

import scala.util.matching.Regex
import scala.collection.mutable.ArrayBuffer


def filterSymbols(str: String): String = {
val emptyChar = ' '
val filteredString = str.map(c => if(Character.isLetter(c) || Character.isDigit(c) ||  c != '+' || c != '-') c.toLower else emptyChar)
filteredString.mkString.replaceAll("\\s"," ")
}

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

val questions = sc.textFile("AllQuestionLemmas.txt").map(_.split(" ").toSeq)
val numquestions = questions.count
import scala.collection.mutable.HashMap

// map terms in each sentence to their count within the sentence
val qTermFreqs = questions.map(terms => {
val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
(map, term) => {
map += term -> (map.getOrElse(term, 0) + 1)
map
}
}
termFreqs
})

// get the Count of each term accross the whole collection of questions
val totalTermFreq = qTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)

// order terms by terms with the highest frequency take top N values
val numTerms = totalTermFreq.count
val ordering = Ordering.by[(String, Int), Int](_._2)
// examining frequent terms
val topTerms = totalTermFreq.top(numTerms)(ordering)
val bottomTerms = totalTermFreq.top(numTerms)(ordering.reverse)

// compute idfs for question terms and take top 10,000
val idfsOrdering = Ordering.by[(String, Double), Double](_._2)
val idfs = totalTermFreq.map{
case (term, count) => (term, math.log(numquestions.toDouble / count))
}
// examining idfs scores for terms from lowest to highest
val topTermsIDFS = idfs.top(numTerms)(idfsOrdering)
val bottomTermsIDFS = idfs.top(numTerms)(idfsOrdering.reverse)

// fiter question terms and map to array then collect
val questionsArray = totalTermFreq.map(term => filterSymbols(term._1)).collect




