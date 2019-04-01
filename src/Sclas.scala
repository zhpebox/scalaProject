import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.random


object Sclas {
 def main(args: Array[String]): Unit = {
  println("Hello, world  test!")
  val conf = new SparkConf()
  conf.setAppName("WorkCount1").setMaster("spark://192.168.0.200:7077").setJars(List("/Users/mobilegis/IdeaProjects/untitledScala/out/artifacts/untitledScala_jar/untitledScala.jar"))

  val spark = new SparkContext(conf)

  val slices = if (args.length > 0) args(0).toInt else 2
  val n = 100000 * slices

  val count = spark.parallelize(1 to n, slices).map { i =>
   val x =  random * 2 - 1
   val y = random* 2 - 1
   if (x * x + y * y < 1) 1 else 0
  }.reduce(_ + _)
  println("Pi is roughly " + 4.0 * count / n)
  spark.stop()
  }
}