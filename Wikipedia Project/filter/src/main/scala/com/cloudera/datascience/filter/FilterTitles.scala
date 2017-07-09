package com.cloudera.datascience.filter

import edu.umd.cloud9.collection.XMLInputFormat
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class FilterTitles(private val spark: SparkSession, private val questions: Set[String], private val stopWordsSet: Set[String], private val wikititles: Set[String]) extends Serializable {
  import spark.implicits._
  val allQuestions = questions
  val bAllQuestions = spark.sparkContext.broadcast(allQuestions).value
  val stopWords = stopWordsSet
  val bStopWords = spark.sparkContext.broadcast(stopWords)
  val titleSet = wikititles
  val btitleSet = spark.sparkContext.broadcast(titleSet).value


def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
  val page = new EnglishWikipediaPage()

  val hackedPageXml = pageXml.replaceFirst(
  "<text xml:space=\"preserve\" bytes=\"\\d+\">", "<text xml:space=\"preserve\">")
  WikipediaPage.readPage(page, hackedPageXml)
  if (page.isEmpty || !page.isArticle || page.isRedirect || page.isDisambiguation ||
    page.getTitle.contains("(disambiguation)")) {
    None
  } else {
  Some((page.getTitle, page.getContent))
  }
}

  def parseWikipediaDump(path: String): Dataset[(String, String)] = {
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    val kvs = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
    val rawXmls = kvs.map(_._2.toString).toDS()
    rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)
  }

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
      if (lemma.length > 1 && !bStopWords.value.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }
  /** 
    The following function filters all collected wikipedia articles by thier existence in the title set obtained from
    Get Articles.
  */
  def contentsToTerms(docs: Dataset[(String, String)]): Dataset[(String, Seq[String])] = {
    docs.mapPartitions { iter =>
      val pipeline = createNLPPipeline()
      iter.map { 
        case (title, contents) => 
        (plainTextToLemmas(title, pipeline).mkString(" "), plainTextToLemmas(contents, pipeline)) 
        }
    }.filter(docTerms => titleSet.contains(docTerms._1))
  }

    def documentTermMatrix(docTexts: Dataset[(String, String)]) = {
    val terms = contentsToTerms(docTexts)

    val termsDF = terms.toDF("title", "terms")
    termsDF.write.save("gs://wallacebigdatabucket/termsDF")

  }

}
