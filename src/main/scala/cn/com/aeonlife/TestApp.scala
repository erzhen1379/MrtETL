package cn.com.aeonlife

import org.apache.spark.{SparkConf, SparkContext}

object TestApp {
  def main(args: Array[String]): Unit = {
    println("11111111")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestApp")
    val sc = new SparkContext(conf)
    val data = sc.textFile("./TestData/word.txt")
    //  data.saveAsTextFile("./TestData/1")
   val aa= data.collect()
    aa.foreach(println)
  }
}
