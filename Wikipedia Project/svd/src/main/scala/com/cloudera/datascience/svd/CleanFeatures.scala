package com.cloudera.datascience.svd

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.tagger._
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import java.util.Properties
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class CleanFeatures(private val spark: SparkSession, private val stopWordsSet: Set[String]) extends Serializable {
  import spark.implicits._
  val stopWords = stopWordsSet
  val bStopWords = spark.sparkContext.broadcast(stopWords).value


def createNLPPipeline(): StanfordCoreNLP = {
val props = new Properties()
props.put("annotators", "tokenize, ssplit, pos, lemma")
new StanfordCoreNLP(props)
}

def isOnlyLetters(str: String): Boolean = {
str.forall(c => (Character.isLetter(c) || Character.isDigit(c) ||  c != '+' || c != '-'))
}

def plainTextToLemmas(text: String, pipeline: StanfordCoreNLP): Seq[String] = {
val doc = new Annotation(text)
pipeline.annotate(doc)
val lemmas = new ArrayBuffer[String]()
val sentences = doc.get(classOf[SentencesAnnotation])
for (sentence <- sentences.asScala;
token <- sentence.get(classOf[TokensAnnotation]).asScala) {
val lemma = token.get(classOf[LemmaAnnotation])
if (lemma.length > 1 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
lemmas += lemma.toLowerCase
}
}
lemmas
}


}
