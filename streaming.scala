import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object streaming {

    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession
        .builder
        .master("local[*]")
        .appName("example")
        .getOrCreate()
      val static = spark.read.json("data/activity-data/")
      val dataSchema = static.schema
      static.printSchema()
      val streaming = spark.readStream.schema(dataSchema)
       .option("maxFilesPerTrigger", 1).json("data/activity-data/")
// selection and filtering
      val transform = streaming.withColumn("stairs",expr("gt like ' %stairs% ' "))
        .where("stairs")
        .where("gt is not null")
        .writeStream
        .queryName("transform")
        .format("memory")
        .outputMode("append")
        .start()
      val device_model = streaming.cube("gt","model").avg()
        .drop("avg(Arrival_Time)")
        .drop("avg(Creation_Time)")
        .writeStream.queryName("device_counts").format("memory").outputMode("complete")
        .start()
      device_model.awaitTermination()
      spark.sql("SELECT *FROM device_counts").show()






    }



}
