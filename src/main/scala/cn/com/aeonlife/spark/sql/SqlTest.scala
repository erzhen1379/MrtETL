package cn.com.aeonlife.spark.sql


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlTest {
  def main(args: Array[String]): Unit = {
    print("1111111")
    val conf = new SparkConf().setAppName("SqlTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("select * from tmp.gr_p_customer").show()
println("111111111----------------")
  }
}
