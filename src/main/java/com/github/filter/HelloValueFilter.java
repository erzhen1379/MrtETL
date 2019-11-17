package com.github.filter;

import com.github.utils.HbaseApi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloValueFilter {
    public static Configuration conf;

    static {
//使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "node168,node169,node170");
        conf.set("hbase.master", "node168");
        conf.set("zookeeper.znode.parent", "/hbase");
    }

    public static void main(String[] args) throws IOException {
        HTable table = new HTable(conf, "staffe");
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("n"));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
        System.out.println("---------");
        HbaseApi.getAllRows("staffe");
    }
}
