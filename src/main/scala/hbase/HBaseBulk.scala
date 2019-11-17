package hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration

/**
  *测试HBaseBulk批量导入
  * 待整理
  */
object HBaseBulk {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Bulk")
    val sc = new SparkContext(sparkConf.set("spark.testing.memory", "512000000"))
    val conf = new Configuration()
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "node168,node169,node170");
    conf.set("hbase.master", "node168");
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "bulktest")
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val init = sc.makeRDD(Array("1,james,32", "2,lebron,30", "3,harden,28"))
    val rdd = init.map(_.split(",")).map(arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    })
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }
}