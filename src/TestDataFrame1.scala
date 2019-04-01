import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//定义case class，相当于表结构
case class People(var name:String, var age:Int)

object TestDataFrame1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Program\\hadoopCommon")
    System.setProperty("HADOOP_USER_NAME","hadoop");
    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("spark://centos1:7077").setJars(List("E:\\workplace\\IDEAworkplace\\IdeaProjects\\scalaProject\\out\\artifacts\\scalaProject_jar\\scalaProject.jar"))
    val sc = new SparkContext(conf);
    val context = new SQLContext(sc);

    //将本地的数据读入RDD，并将RDD与case class关联
    val peopleRDD = sc.textFile("hdfs://centos1:9000/data/people.txt")
      .map(line => People(line.split(",")(0),line.split(",")(1).trim.toInt))

    import context.implicits._

    //将RDD转化成DataFrame
    val df = peopleRDD.toDF
    //将DataFrame创建成一个临时的视图
    df.createOrReplaceTempView("people")
    //使用sql进行查询
    context.sql("select * from people").show()
  }
}
