import org.apache.spark.serializer.KryoSerializer
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
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

val spark = SparkSession.builder().master("local[*]").config("spark.serializer", classOf[KryoSerializer].getName).getOrCreate()
import spark.implicits._
// Read In The Train And Test CSVs
val trainDF = spark.read.option("header", "true").csv("FinalCleanTrain.txt")
val testDF = spark.read.option("header", "true").csv("FinalCleanTest.txt")

def removeBrackets(rowString: String): String = {
val rowSize = rowString.length
val cleanString = rowString.substring(1, rowString.length-1)
cleanString
}

// trainQuestions.count = 808566
val trainQuestions = trainDF.select("question1").map(row => removeBrackets(row.toString())).rdd.union(trainDF.select("question2").map(row => removeBrackets(row.toString())).rdd)
// testQuestions.count = 4691594
val testQuestions = testDF.select("question1").map(row => removeBrackets(row.toString())).rdd.union(testDF.select("question2").map(row => removeBrackets(row.toString())).rdd)
// union questions rdds and convert to dataset
val TestTrainDataset = trainQuestions.union(testQuestions).toDS

// function to create NLOO pipeline
def createNLPPipeline(): StanfordCoreNLP = {
val props = new Properties()
props.put("annotators", "tokenize, ssplit, pos, lemma")
new StanfordCoreNLP(props)
}

// filter out everything but desired characters
def isOnlyLetters(str: String): Boolean = {
str.forall(c => (Character.isLetter(c) || Character.isDigit(c) ||  c != '+' || c != '-'))
}

// convert raw text to lemmas
def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
: Seq[String] = {
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

// lemmatize each question in a question dataset (Altered LSA code, removing )
def lemmatizeQuestions(questions: Dataset[String], stopWordsFile: String): Dataset[Seq[String]] = {
val stopWords = scala.io.Source.fromFile(stopWordsFile).getLines().toSet
val bStopWords = spark.sparkContext.broadcast(stopWords).value
questions.mapPartitions { iter =>
val pipeline = createNLPPipeline()
iter.map { question => plainTextToLemmas(question, stopWords, pipeline) }
}
}
// Lemmatize Questions
val QuestionLemmas = lemmatizeQuestions(TestTrainDataset, "stopwords.txt")

// QuestionLemmasArray.size = 5500160
val QuestionLemmasArray = QuestionLemmas.rdd.map(question => question.mkString(" ")).collect

// filteredQuestionLemmasArray.size = 5475811
val filteredQuestionLemmasArray = QuestionsLemmasArray.filter(line => line.isEmpty == false)

// write lemmatized AllQuestionLemmasToFile
import java.io._
val fw = new FileWriter("AllQuestionLemmas.txt", true)
filteredQuestionLemmasArray.map(question => fw.write(question + "\n"))
fw.close()

// QuestionNotLemmasArray.size = 5500160
val QuestionNotLemmasArray = trainQuestions.union(testQuestions).collect
// filteredQuestionNotLemmasRDD.size = 5500160
val filteredQuestionNotLemmasArray = QuestionNotLemmasArray.filter(line => line.isEmpty == false)

// Write unlemmatized questions to file
val fw2 = new FileWriter("AllQuestions.txt", true)
filteredQuestionNotLemmasArray.map(question => fw2.write(question + "\n"))
fw2.close()


  def contentsToTerms(docs: Dataset[(String, String)]): Dataset[(String, Seq[String])] = {
    docs.mapPartitions { iter =>
      val pipeline = createNLPPipeline()
      iter.map { 
        case (title, contents) => 
        (plainTextToLemmas(title, pipeline).mkString(" "), plainTextToLemmas(contents, pipeline).filter(ts => allQuestions.contains(ts))) 
        }.filter(docTerms => 
          docTerms._1.split(" ").toSeq.map(word =>                                                         
            allQuestions.contains(word)).distinct.contains(false) == false)
    }
  }