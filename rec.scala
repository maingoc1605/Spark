import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
object rec {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("example")
      .getOrCreate()
    //tao dataframe
    val rating = spark.read.textFile("data/sample_movielens_ratings.txt")
      .selectExpr("split(value,'::')as col")
      .selectExpr(
        "cast(col[0] as int) as userId",
        "cast(col[1] as int) as movieId",
        "cast(col[2] as float ) as rating",
        "cast (col[3] as long) as timetamp"
      )
    rating.show(5)
    //chia data train va test
    val Array(training,test)=rating.randomSplit(Array(0.8,0.2))
    val als = new ALS()
      .setMaxIter(5)// maximum number of iteration vs default =10
      .setRegParam((0.01))//
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    println(als.explainParams())
    val alsModel = als.fit(training)
    val prediction = alsModel.transform(test)
    //return a dataframe of  userId, de xuat bo phim cho user cung score
    alsModel.recommendForAllUsers(10).show()
      //.selectExpr("userId","explode(recommendations)").show()
    alsModel.recommendForAllItems(10)
      .selectExpr("movieId","explode(recommendations)")
      .show()
    //evaluator fo recommendation
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(prediction)
    println(s"root mean square error = $rmse")
    //regression metrics
    val regcomparion = prediction.select("rating","prediction")
      .rdd.map(x => (x.getFloat(8).toDouble,x.getFloat(1).toDouble))
    val metric = new RegressionMetrics(regcomparion)
    prediction.filter("movieId == 2")
      .show()

  }
}
