import org.apache.spark._

import scala.math.random
object SparkDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program\\hadoopCommon")
    System.setProperty("HADOOP_USER_NAME","hadoop");
    val masterUrl = "local[1]"
//    val conf = new SparkConf().setAppName("helenApp").setMaster("spark://centos1:7077")
//      .setJars(List("E:\\workplace\\IDEAworkplace\\IdeaProjects\\scalaProject\\out\\artifacts\\scalaProject_jar\\scalaProject.jar"))
//    val sc = new SparkContext(conf)
//
//
//    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ * 3)
//
//    rdd.filter(_ > 10).collect().foreach(println)
//    println(rdd.reduce(_ + _))
//    sc.stop()


    println("Hello, world  test!")
    val conf = new SparkConf()
    conf.setAppName("WorkCount1").setMaster("spark://centos1:7077").setJars(List("E:\\workplace\\IDEAworkplace\\IdeaProjects\\scalaProject\\out\\artifacts\\scalaProject_jar\\scalaProject.jar"))
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
        println("hello world")

  }
}
