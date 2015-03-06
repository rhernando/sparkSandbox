import com.datastax.spark.connector._
import org.apache.spark._
import SparkContext._

/**
 * Created by rhernando on 5/03/15.
 */
object BatchDriver {

  val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("TW Spark").set("spark.cassandra.connection.host", "localhost"))


  // Data from Cassandra
  val twdata = sc.cassandraTable("twspark", "trendurls")

  // most active user
  val usercount = twdata.map(d => (d.getLong("user_id"), 1)).reduceByKey(_+_)
  // TT most mentioned
  val trendsount = twdata.flatMap(d => d.getSet[String]("trends").map(x =>  (x,1) )).reduceByKey(_+_)


}
