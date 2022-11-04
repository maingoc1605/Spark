package sparktutorial
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Spark_RDD {
  def main (args : Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext (conf)
    // load file text
    val data = sc.textFile("./src/data/inputdata.txt")
    data.foreach(println)
    // RDD la kieu du lieu immutable nen khong duoc tang cuong de dam bao du lieu
    // khoi tao RDD tu spark driver
    //khi du lieu duoc khoi tao tu spark driver duoc chia nho thanh nhieu partition
    val data2 = sc.parallelize(Seq(1,2,3,4,5,6,7))
    data2.foreach(println)//collect tat ca du lieu tu partition lai va print,trong qua trinh collect du lieu bi xao tron
    println("print data theo dung thu tu dau vao")
    data2.collect().foreach(println)
    
    //tao rdd tu 1 RDD khac
    // su dung cac ham transdformation: flatmap va map
    //flatmap thuc hien duoi cac tu trong van ban thanh vector bang dau cach
     val data3 = data.flatMap(ele=>ele.split(" ")).map(x => (x,1))
     .sortByKey(true)
     .reduceByKey(_+_)
     data3.foreach(println)// foreach println la 1 action
     
  }
}