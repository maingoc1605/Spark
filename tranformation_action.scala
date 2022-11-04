package sparktutorial

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

object tranformation_action {
  def main (args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))
    val data2 = sc.parallelize(Seq(100,200,300,400))
    val data3 = sc.parallelize(List("mom","mom","coffe","coffe","film"))
    val result = data1.filter(x => x !=1)
    val data1_sample = data1.sample(false,0.6)
    val data3_map = data3.map(x => x*2)
    data3_map.collect().foreach(println)
    println(data3_map.collect().mkString(","))
    result.collect().foreach(println)
    println("-----------sample--------------")
    println(data1_sample.collect().mkString(","))
    println("________union__________")
    val union = data1.union(data2)
    println(union.collect().mkString(","))
    println("___________reduce______")
    val sum = data2.reduce((x,y)=> x+y)
    val min = data2.reduce((x,y) => x min y)
    val max = data2.reduce((x,y) => x max y)
    println("sum: "+sum)
    println("max: "+max)
    println("min: "+min)
    
   
    val data_text = sc.textFile("./src/data/inputdata.txt")
    val dataflatmap = data_text.flatMap(ele => ele.split("\\W+")).map(x => (x,1))
    
    println("__________groupByKey__________")
    val Count_word_groupbykey = dataflatmap.groupByKey().map(ele => (ele._1,ele._2.sum))
    println(Count_word_groupbykey.collect().mkString(","))
    
    println("__________reduceByKey__________")
    val count_word_reducebykey = dataflatmap.reduceByKey((x,y) =>(x+y))
    println(count_word_reducebykey.collect().mkString(","))
  }
  
}