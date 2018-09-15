package com.chongzi.es;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataSet写入ES示例
 */
public class EsDateSetDemo {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.准备数据
        DataStream<String> input = env.fromElements("a","b","c");

        //3.将数据写入到自定义的sink中（这里是es）
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-one");
        //该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "1000");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.48"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.6"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.36"), 9300));


        //4.触发流执行
        env.execute();
    }
}
