import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object trans {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("example")
      .getOrCreate()
    val df = spark.read.format("csv")
      .option("header","true")
    .option("inferSchema","true")
      .load("data/retail-data/all/online-retail-dataset.csv")
      .coalesce(5)
    df.cache()
    df.createTempView("dfTable")
    df.show()
    df.select(count("CustomerID")).show()
    df.select(count(1)).show()
  }
}

