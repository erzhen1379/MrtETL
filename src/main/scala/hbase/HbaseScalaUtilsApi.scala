package hbase

import java.io.IOException
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete, HBaseAdmin, HTable, Put, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object HbaseScalaUtilsApi {
  //创建hbase的链接信息
  val hBaseconf = HBaseConfiguration.create
  hBaseconf.set("hbase.zookeeper.property.clientPort", "2181")
  hBaseconf.set("hbase.zookeeper.quorum", "node168,node169,node170")
  hBaseconf.set("hbase.master", "node168")
  hBaseconf.set("zookeeper.znode.parent", "/hbase")
  val connection = ConnectionFactory.createConnection(hBaseconf)
  val hAdmin = connection.getAdmin

  /**
    * 判断表是否存在
    *
    * @param tableName
    * @return true or false
    */
  def isTableExist(tableName: String): Boolean = {
    var result = false
    val tName = TableName.valueOf(tableName)
    if (hAdmin.tableExists(tName)) {
      result = true
      println("表已经存在")
    } else {
      println("表不存在")
    }
    result
  }

  /**
    * 创建表
    *
    * @param tableName
    * @param columnFamilys
    */
  def createTable(tableName: String, columnFamilys: Array[String]) = {
    //操作的表名
    val tName = TableName.valueOf(tableName)
    //当表不存在的时候创建Hbase表
    if (!hAdmin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)
      //创建列簇i  此处是通过数组来封装多个列族
      for (columnFamily <- columnFamilys) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }
      //创建表
      hAdmin.createTable(descriptor)
      println("create successful!!")
    } else {
      println("create fail!!")
    }
  }

  /**
    * 删除一张表
    *
    * @param tableName
    */
  def dropTable(tableName: String): Boolean = {
    var status = false
    val tName = TableName.valueOf(tableName)
    if (hAdmin.tableExists(tName)) {
      hAdmin.disableTable(tName)
      hAdmin.deleteTable(tName)
      println("drop successful!!")
      status = true
    } else {
      println("drop fail!!")
    }
    false
  }

  /**
    * put一条数据
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param column
    * @param value
    */
  def addRowData(tableName: String, rowkey: String, columnFamily: String, column: String, value: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
    //把数据插入到tbale中
    table.put(puts)
    println("put successful!!")
  }

  /**
    * 查看表中某一列族下列的所有数据
    * scan( "staffe" "cf1" "name")
    *
    * @param tableName
    * @param columnFamily
    * @param column
    * @return
    */
  def getAllColumnRows(tableName: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //定义scan对象
    val scan = new Scan()
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }

  /**
    * 获得表所有数据
    *
    * @param tableName
    * @return
    */
  def getAllRows(tableName: String): ListBuffer[String] = {
    var table: Table = null
    val list = new ListBuffer[String]
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val results: ResultScanner = table.getScanner(new Scan)
      import scala.collection.JavaConversions._
      for (result <- results) {
        for (cell <- result.rawCells) {
          val row: String = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          val family: String = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val colName: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val context: String = "rowkey:" + row + "," + "列族:" + family + "," + "列:" + colName + "," + "值:" + value
          list += context
        }
      }
      results.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    list
  }


  /**
    * 删除某列
    * delete "staffe" "1001" "cf1" "name"
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param column
    */
  def deleteRecord(tableName: String, rowkey: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val info = new Delete(Bytes.toBytes(rowkey))
    info.addColumn(columnFamily.getBytes(), column.getBytes())
    table.delete(info)
    println("delete successful!!")
  }

  /**
    * 删除多列
    *
    * @param tableName
    * @param rows
    */
  def delMultiRows(tableName: String, rows: Array[String]): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val deleteList = for (row <- rows) yield new Delete(Bytes.toBytes(row)) //yield 会把当前的元素记下来，保存在集合中
    deleteList.foreach(x => {
      table.delete(x)
    })
    println("删除多列成功")

  }

  // 关闭 connection 连接
  def close() = {
    if (connection != null) {
      try {
        connection.close()
        println("关闭成功!")
      } catch {
        case e: IOException => println("关闭失败!")
      }
    }
  }


  def main(args: Array[String]): Unit = {
    // isTableExist("tes2")
      createTable("bulktest", Array("cf1"))
   // dropTable("bulktest")
 //   addRowData("staffe", "1001", "cf1", "name", "nick")
  //  addRowData("staffe", "1002", "cf1", "name", "1002")
  //  addRowData("staffe", "1003", "cf1", "name", "1003")
  //  getAllColumnRows("staffe", "cf1", "name")
/*       val allRows: ListBuffer[String] = getAllRows("test_log")
       allRows.foreach(x => {
         println(x)
       })*/
    // deleteRecord("staffe", "1001", "cf1", "name")
    // delMultiRows("staffe", Array("1002", "1003"))
    close()
  }
}
