import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TestDataFrame2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program\\hadoopCommon")
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    val conf = new SparkConf().setAppName("RDDTODMysql").setMaster("spark://centos1:7077").setJars(List("E:\\workplace\\IDEAworkplace\\IdeaProjects\\scalaProject\\out\\artifacts\\scalaProject_jar\\scalaProject.jar"))
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)

    val url = "jdbc:mysql://192.168.189.1:3306/generatordb"
    val table = "t_module"
    val pro = new Properties()

    pro.setProperty("user", "root")
    pro.setProperty("password", "123456")

    val df = context.read.jdbc(url,table,pro)

    df.createOrReplaceTempView("dbst")
    context.sql("select * from dbst").show()
  }
}