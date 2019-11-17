package test

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    println("11111111")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(conf)
    val data = sc.textFile("./TestData/word.txt")
   //  data.saveAsTextFile("./TestData/1")
    data.foreach(println)
  }
}
