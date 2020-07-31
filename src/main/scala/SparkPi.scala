


import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.SparkSession


//--------------------------------------------

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]): Unit = {
    
    
        val conf = new SparkConf()
        conf.setAppName("SparkPi-App")
        //conf.setMaster("k8s://https://0DA490E3487C1BD790CDB2B8B353CB27.sk1.eu-west-3.eks.amazonaws.com:443")
  
        //conf.setMaster("local[*]")
   
        
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
      
      
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}