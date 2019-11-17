package com.github.corprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author: xianghu.wang
 * @Date: 2018/3/15
 * @Description:
 */
public class HelloWorldObserver extends BaseRegionObserver {

    public static final String JACK = "JACK";

    /**
     * 重写prePut方法。
     * 这个方法会在Put动作之前进行操作
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put, WALEdit edit, Durability durability) throws IOException {

        //获取mycf:name的单元格
        List<Cell> name = put.get(Bytes.toBytes("mycf"), Bytes.toBytes("name"));

        //如果该put中不存在mycf:name,则不作操作，这个单元格直接返回
        if (name == null || name.size() == 0) {
            return;
        }

        //如果该put中存在mycf:name,则判断mycf:name是否为JACK
        if (JACK.equals(Bytes.toString(CellUtil.cloneValue(name.get(0))))) {
            //如果mycf:name是JACK，则在mycf:message中添加一句话
            put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("message"), Bytes.toBytes("Hello World! Welcome back !"));
        }

    }
}