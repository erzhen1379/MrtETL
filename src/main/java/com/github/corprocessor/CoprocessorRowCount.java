package com.github.corprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


/**
 * 测试协处理器
 * alter 'mytable', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.example.RowCountEndpoint||'
 */
public class CoprocessorRowCount {
    public static Configuration conf;

    static {
//使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "node168,node169,node170");
        conf.set("hbase.master", "node168");
        conf.set("zookeeper.znode.parent", "/hbase");
    }

    public static void main(String[] args) throws Throwable {
        HTable table = new HTable(conf, "mytable");
        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
        Map<byte[], Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
                null, null,
                new Batch.Call<ExampleProtos.RowCountService, Long>() {
                    public Long call(ExampleProtos.RowCountService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
                                new BlockingRpcCallback<ExampleProtos.CountResponse>();
                        counter.getRowCount(controller, request, rpcCallback);
                        ExampleProtos.CountResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasCount()) ? response.getCount() : 0;
                    }
                });
        for (byte[] key:results.keySet()){
            String str= new String (key);
            System.out.println(str);
        }
        long sum = 0;
        int count = 0;
        for (Long l : results.values()) {
            sum += 1;
            count++;
            System.out.println(results.values());
        }
        System.out.println("row count= " + sum);
        System.out.println("region count =" + count);
    }
}
