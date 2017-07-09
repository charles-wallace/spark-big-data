import org.apache.spark.rdd.RDD
import scala.collection.Map._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.recommendation.Rating


val rawUserArtistData: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/user_artist_data.txt")
val rawArtistData: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/artist_data.txt")
val rawArtistAlias: org.apache.spark.rdd.RDD[String] = sc.textFile("profiledata_06-May-2005/artist_alias.txt")


