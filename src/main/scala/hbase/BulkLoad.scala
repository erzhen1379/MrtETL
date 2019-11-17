package hbase

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BulkLoad {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().set("spark.testing.memory", "512000000")
      .setMaster("local[12]")
      .setAppName("BulkLoad")
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val datas = List(
      ("abc", ("ext", "type", "login")),
      ("ccc", ("ext", "type", "logout"))
    )
    val dataRdd = sc.parallelize(datas)
    val output = dataRdd.map {
      x => {
        val rowKey = Bytes.toBytes(x._1)
        /**
          * Create a ImmutableBytesWritable using the byte array as the initial value.
          * This array becomes the backing storage for the object.
          */
        val immutableRowKey = new ImmutableBytesWritable(rowKey)
        //打印该值，看看返回的，
        val colFam = x._2._1
        val colName = x._2._2
        val colValue = x._2._3
        val kv = new KeyValue(   //存储格式为：（rowkey,colFamily,colName,value）
          rowKey,
          Bytes.toBytes(colFam),
          Bytes.toBytes(colName),
          Bytes.toBytes(colValue.toString)
        )
        (immutableRowKey, kv)
      }
    }
    val hConf = HBaseConfiguration.create()
    hConf.addResource("hbase-site.xml")
    val hTableName = "test_log"
    HbaseScalaUtilsApi.createTable(hTableName, Array("ext"))
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hTableName)
    val tableName = TableName.valueOf(hTableName)
    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(tableName)
    val regionLocator = conn.getRegionLocator(tableName)
    val jobId = UUID.randomUUID().toString
    val hFileOutput = s"/tmp/bulkload/$jobId"   //此目录设置的是hdfs目录
    output.saveAsNewAPIHadoopFile(hFileOutput,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf
    )
    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path(hFileOutput), conn.getAdmin, table, regionLocator)
    HbaseScalaUtilsApi.getAllRows("test_log")
    println("aaaaaaa")
  }

}