package cn.com.aeonlife.etl.customer

import cn.com.aeonlife.etl.domain.CustomerBean
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 进行入库数据进行清洗
  */
object CustomerEtl {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf()
      .setAppName("CustomerEtl")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://master01:8020/MRT/TMP/GR_P_CUSTOMER/part-m-00000", 6)
    //   lines.foreach(println)
    //   val lines = sc.textFile("D:\\MrtETL\\TestData\\part-00000", 1) //获取客户信息
    //   val city = sc.textFile("D:\\MrtETL\\TestData\\part-m-city") //获取城市的码表
    val splrdd = lines.map(lines => {
      val line = lines.split("\001")
      line.foreach(println)
      val f_key:String = line(0)
      val f_inserttime = line(1)
      val f_upDatetime = line(2)
      val f_isobsolete = line(3)
      val f_code = line(4)
      val f_specialflag = line(5)
      val f_selfdata = line(6)
      val f_branch = line(7)
      val f_commender = line(8)
      val f_name = line(9)
      val f_nationality = line(10)
      val f_gender = line(11)
      val f_birthday = line(12)
      val f_age = line(13)
      val f_marriage = line(14)
      val f_education = line(15)
      val f_documenttype = line(16)
      val f_document = line(17)
      val f_email = line(18)
      val f_mobile = line(19)
      val f_companyname = line(20)
      val f_companycity = line(21)
      val f_companydistrict = line(22)
      val f_companyaddress = line(23)
      println(f_key + "::" + f_inserttime + "::")
    }).collect()
    //   splrdd.saveAsTextFile("hdfs://master01:8020/erzhen/5")

  }
}
