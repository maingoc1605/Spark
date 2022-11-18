
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions.{col, concat, count, desc, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object flight {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("example")
      .getOrCreate()
   val path ="data/1987.csv"
    val flight = spark.read
      .option("header","true")
      .csv(path)
    flight.show(5)
    flight.createTempView("flights")
    flight.printSchema()
    //tao them 1 cot moi the hien diem di_diem den
    val df1 = flight.withColumn("orig_dest",concat(col("Origin"),lit("_"),col("Dest")))
    df1.show(5)
   val df2 =df1.select(col("orig_dest"),col("DepDelay"))
      .filter("DepDelay>40")
     .groupBy("orig_dest")
     .count()
     .orderBy(desc("count"))
    df2.select("count")

val categotical = Array("carrier")











  }

}