package cn.com.aeonlife.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveContextSelect {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]");
    conf.setAppName("HiveContextSelect")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    sql("show databases")
    val rdd = sql("select * from tmp.gr_p_customer limit 10").rdd





    // sc.stop()
  }
}