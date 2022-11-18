package sparktutorial
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
//predict price given the number of bedrooms
object ML {
   def main (args : Array[String]): Unit ={
     Logger.getLogger("org").setLevel(Level.ERROR)
     val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("example")
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
     val filePath =
 "./databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet"
val airbnbDF = spark.read.parquet(filePath)
airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
 "number_of_reviews", "price").show(5)
 //chia data thanhf tap train vaf tap val
 val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8,.2),seed =42)
println(f"""There are ${trainDF.count} rows in the training set, and 
${testDF.count} in the test set""")
trainDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
 "number_of_reviews", "price").show(5)
val vecAssembler = new VectorAssembler()
 .setInputCols(Array("bedrooms"))
 .setOutputCol("features")
val vecTrainDF = vecAssembler.transform(trainDF)
vecTrainDF.select("bedrooms", "features", "price").show(10)
val lr = new LinearRegression()
     .setFeaturesCol("features")
     .setLabelCol("price")
 val lrModel = lr.fit(vecTrainDF)
 val m = lrModel.coefficients(0)
 val b = lrModel.intercept
 println(f"""the formula for the linear regression line is price = $m%1.2f* bedrooms +$b%1.2f""")
 // if we want to apply model to our test set, wwe need to prepare that data in the same wway with training data
     // creating a pipeline
     //n. In Spark, Pipelines are esti‐mators, whereas PipelineModels—fitted Pipelines—are transformers.
  val pipeline = new Pipeline().setStages(Array(vecAssembler,lr))
  val pipelineModel = pipeline.fit(trainDF)
  val predDF = pipelineModel.transform(testDF)
predDF.select("bedrooms","features","price","prediction").show(10)
 val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
 val indexOutputCols = categoricalCols.map(_ + "Index")
 val oheOutputCols = categoricalCols.map(_ +"OHE")
 
 val stringIndex = new StringIndexer()
     .setInputCols(categoricalCols)
     .setOutputCols(indexOutputCols)
     .setHandleInvalid("skip")
     
 val oheEncoder = new OneHotEncoder()
     .setInputCols(indexOutputCols)
     .setOutputCols(oheOutputCols)
     
 val numericCols = trainDF.dtypes.filter{ case (field, dataType) => dataType == "DoubleType" && field != "price"}.map(_._1)
 val assemblerInputs = oheOutputCols ++ numericCols
 val vecAssembler_1 = new VectorAssembler()
 .setInputCols(assemblerInputs)
 .setOutputCol("features")
 // the feature in repreaented as a sparseVector với 98 feature sau khi one hot endcoder
 val lr_1 =new LinearRegression()
 .setLabelCol("price")
 .setFeaturesCol("features")
 val pipeline_1 = new Pipeline()
 .setStages(Array(stringIndex, oheEncoder, vecAssembler_1, lr_1))

val pipelineModel_1 = pipeline_1.fit(trainDF)
val predDF_1 = pipelineModel_1.transform(testDF)
predDF_1.select("features", "price", "prediction").show(5)

 
   }
   
   
}