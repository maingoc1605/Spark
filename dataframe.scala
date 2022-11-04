package sparktutorial

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object dataframe {
  def main (args : Array[String]): Unit ={
    val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("example")
    .getOrCreate()
    val delaysPath ="./databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
    val airportsPath ="./databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
    val airport = spark.read
    .option("header","true")
    .option("inferchema","true")
    .option("delimiter","\t")
    .csv(airportsPath)
    airport.createOrReplaceTempView("airports_na")
    val delays = spark.read
    .option("header","true")
   .csv(delaysPath)
   .withColumn("delay", expr("CAST(delay as INT) as delay"))
   .withColumn("distance", expr("CAST(distance as INT) as distance"))
  delays.createOrReplaceTempView("departureDelays")
  val foo = delays.filter(
   expr("""origin == 'SEA' AND destination == 'SFO' AND 
   date like '01010%' AND delay > 0"""))
  foo.createOrReplaceTempView("foo")
  //println("_______________airports__________")
  //park.sql("SELECT * FROM airports_na LIMIT 10").show()
  //println("_____________flight______________")
  //spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
  println("_____flight from SEA to SFO with delay____")
  spark.sql("SELECT * FROM foo").show()
  //adding new columns
  val foo2 = foo.withColumn("satus",expr("CASE WHEN delay <=10 THEN 'on-time' ELSE 'delay' END") )
  foo2.show()
  //inner joins
  val foo3 = foo.join(airport,  foo("origin") === airport("IATA"))
  foo3.show()
 // drop column
  val foo4 = foo2.drop("delay")
  foo4.show()
val foo5 = foo4.withColumnRenamed("status", "flight_status")
foo5.show()

  }
  
}