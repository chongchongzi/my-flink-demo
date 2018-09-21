package com.chongzi.es;

import com.chongzi.mysql.Student;
import com.chongzi.mysql.StudentSinkToMysql;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataStream写入ES示例
 */
public class EsDataStreamDemo {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.准备数据
        DataStream<String> input = env.fromElements("d","e","f");

        //3.将数据写入到自定义的sink中（这里是es）
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-one");
        //该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "1");
        //1、用来表示是否开启重试机制
        config.put("bulk.flush.backoff.enable", "true");
        //2、重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        //config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
        config.put("bulk.flush.backoff.type", "CONSTANT");
        //3、进行重试的时间间隔。对于指数型则表示起始的基数
        config.put("bulk.flush.backoff.delay", "2");
        //4、失败重试的次数
        config.put("bulk.flush.backoff.retries", "3");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.48"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.6"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.36"), 9300));

        input.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                //将需要写入ES的字段依次添加到Map当中
                json.put("data", element);

                return Requests.indexRequest()
                        .index("chongzi")
                        .type("chongzi-type")
                        .source(json);
            }

            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));

        //4.触发流执行
        env.execute();
    }
}
