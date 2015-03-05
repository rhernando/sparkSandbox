import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import com.datastax.spark.connector.streaming._
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder
import StreamingContext._

/**
 * Created by rhernando on 26/02/15.
 */
object StreamDriver {

  def main(args: Array[String]) {

    val cb = new ConfigurationBuilder().setUseSSL(true)
    val tf = new TwitterFactory(cb.build())
    val twitterApi = tf.getInstance()

    val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("TW Spark").set("spark.cassandra.connection.host", "localhost"), Seconds(1))
    ssc.checkpoint("tmp/")

    // Global information is available by using 1 as the WOEID.
    val trends = twitterApi.getPlaceTrends(1).getTrends.map(trend => trend.getName)
    val stream = TwitterUtils.createStream(ssc, None, trends)

    // get status with url
    //stream.filter(status => status.getURLEntities().length > 0).map(status => status.getText).print()
    val urlTweets = stream.filter(status => status.getURLEntities().length > 0).persist()

    val updateCountFun = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val stateCounter = urlTweets.flatMap(status => status.getURLEntities.map(urlent => (urlent.getExpandedURL, 1 ))).updateStateByKey(updateCountFun)
    stateCounter.print()


    val rowtrends = urlTweets.flatMap(tweet => tweet.getURLEntities.map(urlent => (urlent.getExpandedURL, tweet.getId, tweet.getUser.getId, tweet.getCreatedAt, tweet.getText)) )
    //rowtrends.print()

    rowtrends.saveToCassandra("twspark", "trendurls", SomeColumns("url", "tweet", "user_id", "produced", "trend"))


    //val userRdd = ssc.cassandraTable("twspark", "users").select("twusername")
    //userRdd.collect().foreach(println)

    ssc.start()
    ssc.awaitTermination()
  }
}
