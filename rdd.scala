import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object rdd {
  def main (args : Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data2 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7))
    data2.foreach(println)


  }
}