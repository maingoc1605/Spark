import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object retail {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("example")
      .getOrCreate()
    val static = spark.read.json("data/activity-data/")
    val dataSchema = static.schema
    val streaming = spark.readStream.schema(dataSchema)
      .option("maxFilesPerTrigger", 1).json("data/activity-data/")

    val activityCounts = streaming.groupBy("gt").count()
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    val activityQuery = activityCounts.writeStream.queryName("activity_counts")
      .format("memory").outputMode("complete")
      .start()
    spark.streams.active
    activityQuery.awaitTermination(60000)
    for(i<- 1 to 5) {
      spark.sql("SELECT  * FROM  activity_counts").show()
      Thread.sleep(1000)
    }


  }

}
