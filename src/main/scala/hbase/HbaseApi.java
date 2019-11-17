package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseApi {
    public static Configuration conf;

    static {
//使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hb-proxy-pub-uf686q2gp6v270oq7-001.hbase.rds.aliyuncs.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    /**
     * 创建表空间
     *
     * @param namespaceNameStr
     * @throws IOException
     */
    public static void createNamespace(String namespaceNameStr) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespaceNameStr);
        NamespaceDescriptor nd = builder.build();
        admin.createNamespace(nd);
        System.out.println("创建命" + namespaceNameStr + "名空间成功");
    }
    /**
     * 删除命名空间
     *
     * @param namespaceNameStr
     * @throws IOException
     */
    public static void deleteNamespace(String namespaceNameStr) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        admin.deleteNamespace(namespaceNameStr);
        System.out.println("删除命名空间" + namespaceNameStr + "成功");
    }

    /**
     * 查看所有命名空间
     *
     * @param namespaceNameStr
     * @throws IOException
     */
    public static void scanAllNamespace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor name : list) {
            System.out.println("命名空间：" + name);
        }
    }

    /**
     * 判读表是否存在(新的api)
     *
     * @param tableName
     * @return true or false
     */
    public static boolean isTableExist(String tableName) throws IOException {
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 注意老版本的api 判断表是否存在
     * 老的api兼容性好（一定范围），
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExistOld(String tableName) throws IOException {
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
        //注意这是老版本的api
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 创建一张表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        //管理表用的是admin  ，管理表的数据getTable
        HBaseAdmin admin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            //HTableDescriptor 表的描述器
            //创建表需要的是列族，不需要表
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    /**
     * 进行单条数据插入
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws
            IOException {
        //创建 HTable 对象
        HTable hTable = new HTable(conf, tableName);   //老的api
       /* Connection connection = ConnectionFactory.createConnection(conf);//新的api
        Table table = connection.getTable(TableName.valueOf(tableName));*/
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));    //要求是字节数组,转换为(Bytes.toBytes)
        //向 Put 对象中组装数据
        //put.add() 封装到一个新的单元格
        //用于添加多条数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
      /* //单条数据，已经过期
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));*/
        hTable.put(put);
        //  hTable.put(Put);  put对象
        // hTable.put();  list集合
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除多行数据
     *
     * @param tableName
     * @param rows(根据业务可以将方法重载)
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
        System.out.println("删除数据成功");
    }

    /**
     * 获得所有行的数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        // scan.setMaxVersions();  扫描最大的版本号
        //使用 HTable 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到 rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取rowKey下某一行的数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /**
     * 获取某一行指定“列族:列”的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }





    public static void main(String[] args) throws IOException {
    //  scanAllNamespace();
/*        createNamespace("aa");
        scanAllNamespace();
        deleteNamespace("aa");*/
        //1测试isTableExist
            System.out.println(isTableExist("TEST_HBASE_PHOENIX"));
        //  createTable("staffe", "cf1", "cf2");
        //  dropTable("staffe");
        // System.out.println(isTableExist("staffe"));
        // addRowData("staffe", "1001", "cf1", "name", "nick");
        //  addRowData("staffe", "1002", "cf1", "name", "1002");
        //  addRowData("staffe", "1003", "cf1", "name", "1003");
        //   deleteMultiRow("staffe", "cf1", "1001", "1002");
        //  getAllRows("staffe");
        //  getRow("staffe", "1001");
        //  getRowQualifier("staffe", "1001", "cf1", "name");


    }
}
