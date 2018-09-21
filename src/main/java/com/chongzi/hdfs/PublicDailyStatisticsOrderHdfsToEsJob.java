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
import java.util.*;

public class PublicDailyStatisticsOrderHdfsToEsJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<PublicDailyStatisticsOrder> csvInput = env
                .readCsvFile("hdfs:///input/ns1/usr/local/services/hive/warehouse/ips.db/public_daily statistics_order")
                .pojoType(PublicDailyStatisticsOrder.class, "id", "the_date_cd", "sku", "spu", "website_code", "site_code", "terminal", "country", "real_qty", "order_qty", "sale_account_usd", "etl_date", "dw_sys_date");

        //3.将数据写入到自定义的sink中（这里是es）
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-one");
        //该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "10000");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.48"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.6"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.60.34.36"), 9300));

        csvInput.output(new ElasticSearchOutputFormat<>(config, transportAddresses, new ElasticsearchSinkFunction<PublicDailyStatisticsOrder>(){
            @Override
            public void process(PublicDailyStatisticsOrder element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }

            public IndexRequest createIndexRequest(PublicDailyStatisticsOrder obj) {
                Map<String, Object> json = new HashMap<>();
                //将需要写入ES的字段依次添加到Map当中
                json.put("id", obj.getId());
                json.put("the_date_cd",obj.getThe_date_cd());
                json.put("sku",obj.getSku());
                json.put("spu",obj.getSpu());
                json.put("website_code",obj.getWebsite_code());
                json.put("site_code",obj.getSite_code());
                json.put( "terminal",obj.getTerminal());
                json.put("country",obj.getCountry());
                json.put("real_qty",obj.getReal_qty());
                json.put("order_qty",obj.getOrder_qty());
                json.put( "sale_account_usd",obj.getSale_account_usd());
                json.put("etl_date",obj.getEtl_date());
                json.put("dw_sys_date",obj.getDw_sys_date());
                return Requests.indexRequest()
                        .index("chongzi")
                        .type("public_daily statistics_order")
                        .source(json);
            }
        }));

        env.execute();

    }
}
