package com.chongzi.hdfs;

import com.chongzi.es.ElasticSearchOutputFormat;
import com.chongzi.es.ElasticsearchSinkFunction;
import com.chongzi.es.RequestIndexer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsToEsDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Record> csvInput = env
                .readCsvFile("hdfs:///input/flink/olympic-athletes.csv")
                .pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
        
        //3.将数据写入到自定义的sink中（这里是es）
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-one");
        //该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "10000");
        //1、用来表示是否开启重试机制
        //config.put("bulk.flush.backoff.enable", "true");
        //2、重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        //config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
        //config.put("bulk.flush.backoff.type", "CONSTANT");
        //3、进行重试的时间间隔。对于指数型则表示起始的基数
        //config.put("bulk.flush.backoff.delay", "2");
        //4、失败重试的次数
        //config.put("bulk.flush.backoff.retries", "3");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.48"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.6"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.36"), 9300));

        csvInput.output(new ElasticSearchOutputFormat<>(config, transportAddresses, new ElasticsearchSinkFunction<Record>(){
            @Override
            public void process(Record element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }

            public IndexRequest createIndexRequest(Record obj) {
                Map<String, Object> json = new HashMap<>();
                //将需要写入ES的字段依次添加到Map当中
                json.put("playerName", obj.getPlayerName());
                json.put("country",obj.getCountry());
                json.put("year",obj.getYear());
                json.put("game",obj.getGame());
                json.put("gold",obj.getGold());
                json.put("silver",obj.getSilver());
                json.put("bronze",obj.getBronze());
                json.put("total",obj.getTotal());
                return Requests.indexRequest()
                        .index("chongzi")
                        .type("record")
                        .source(json);
            }
        }));

        env.execute();

    }
}
