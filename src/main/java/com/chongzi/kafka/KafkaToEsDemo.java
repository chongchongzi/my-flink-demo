package com.chongzi.kafka;

import cn.hutool.core.util.StrUtil;
import com.chongzi.bean.Product;
import com.chongzi.event.*;
import com.chongzi.log.util.AppLogConvertUtil;
import com.chongzi.log.util.GsonUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 模拟kafka日志数据写入es
 * @Author chongzi
 * @Date 2018/10/8 17:30
 **/
public class KafkaToEsDemo {
    public static void main(String args[]) throws Exception {
        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);
        String clusterName = "esearch-ai-aws";
        String esTransPorts = "127.0.0.1:9300,127.0.0.1:9300,127.0.0.1:9300";
        String esIndex = "chongzi";
        String esType = "product";
        //3.将数据写入到自定义的sink中（这里是es）
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", clusterName);
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
        for (String s : esTransPorts.split(",")) {
            String[] transPort = s.split(":");
            transportAddresses.add(new InetSocketAddress(transPort[0], Integer.parseInt(transPort[1])));
        }

        DataStream<String> messageStream = see.fromElements("127.0.0.1^A^-^A^[08/Oct/2018:07:04:23 +0000]^A^\"GET /_app.gif?re_targeting_conversion_type=&is_retargeting=false&app_id=com.zaful&platform=android&event_type=in-app-event&attribution_type=organic&event_time=2018-10-08%2007%3A04%3A22&event_time_selected_timezone=2018-10-08%2015%3A04%3A22.509%2B0800&event_name=af_impression&event_value=%7B%22af_content_id%22%3A%22226385802%2C276932601%2C279767801%2C278595702%2C233930401%2C279781303%2C226386103%2C279846802%2C236254603%2C226385102%2C256051301%2C257337701%22%2C%22af_inner_mediasource%22%3A%22recommend%20productdetail%22%2C%22af_version_id%22%3A%22234%22%2C%22af_plan_id%22%3A%2289%22%2C%22af_bucket_id%22%3A%222%22%7D&currency=USD&selected_currency=USD&revenue_in_selected_currency=&cost_per_install=&click_time=&click_time_selected_timezone=&install_time=2018-09-09%2019%3A44%3A16&install_time_selected_timezone=2018-09-10%2003%3A44%3A16.131%2B0800&agency=&media_source=Organic&campaign=&fb_campaign_id=&fb_campaign_name=&fb_adset_name=&fb_adset_id=&fb_adgroup_id=&fb_adgroup_name=&af_siteid=&country_code=IN&city=Nagpur%20City&ip=106.210.247.27&wifi=false&language=English&appsflyer_device_id=1536522249835-4170699118690066416&customer_user_id=0&android_id=&imei=&advertising_id=cbce21e9-b091-4205-85ae-0ea5bcd6e74c&mac=&device_brand=samsung&device_model=SM-G610F&os_version=6.0.1&sdk_version=v4.8.14&app_version=3.6.1&operator=airtel&carrier=airtel&af_sub1=&af_sub2=&af_sub3=&af_sub4=&af_sub5=&click_url=&http_referrer=&app_name=Zaful%20-%20Your%20Way%20to%20Save%20on%20Black%20Friday&download_time=2018-09-09%2019%3A40%3A54&download_time_selected_timezone=2018-09-10%2003%3A40%3A54.000%2B0800&af_keywords=&bundle_id=com.zaful&attributed_touch_type=&attributed_touch_time= HTTP/1.1\"^A^200^A^372^A^\"-\"^A^\"http-kit/2.0\"^A^s.logsss.com^A^34.240.29.179, 10.221.222.58, 88.221.222.146, 10.55.59.245, 23.55.59.236^A^34.240.29.179^A^US^A^United States^A^-^A^1538982263");
        //DataStream<String> messageStream = see.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties));
        DataStream<Product> stringDataStream = messageStream.filter(s -> s.contains("/_app.gif?")).flatMap(new Tokenizer());
        String finalEsIndex = esIndex;
        String finalEsType = esType;
        stringDataStream.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<Product>() {
            public IndexRequest createIndexRequest(Product product) {

                return Requests.indexRequest()
                        .index(finalEsIndex)
                        .type(finalEsType)
                        .source(GsonUtil.toJson(product));
            }
            @Override
            public void process(Product product, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(product));
            }
        }));
        //stringDataStream.print();
        see.execute("KafkaToEsDateDemo");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Product> {
        @Override
        public void flatMap(String value, Collector<Product> out) {
            String json = GsonUtil.toJson(AppLogConvertUtil.getAppLogParameters(value));
            Record record = GsonUtil.readValue(json, Record.class);
            String app_name = record.getApp_name();
            String platform = record.getPlatform();
            String event_time = record.getEvent_time();
            String event_name = record.getEvent_name();
            String event_value = record.getEvent_value();
            String sku = null;
            Integer num = 0;
            if(event_name.equalsIgnoreCase(EventConstants.AF_IMPRESSION)){
                AfImpression obj = GsonUtil.readValue(event_value, AfImpression.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 曝光数
                num = 1;

            }
            if(event_name.equalsIgnoreCase(EventConstants.AF_VIEW_PRODUCT)){
                AfViewProduct obj = GsonUtil.readValue(event_value, AfViewProduct.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 点击数
                num = 1;
            }
            if(event_name.equalsIgnoreCase(EventConstants.AF_ADD_TO_BAG)){
                AfAddToBag obj = GsonUtil.readValue(event_value, AfAddToBag.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 加购数
                num = 1;
            }
            if(event_name.equalsIgnoreCase(EventConstants.AF_ADD_TO_WISHLIST)){
                AfAddToWishlist obj = GsonUtil.readValue(event_value, AfAddToWishlist.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 加收藏数
                num = 1;
            }

            if(event_name.equalsIgnoreCase(EventConstants.AF_CREATE_ORDER_SUCCESS)){
                AfCreateOrderSuccess obj = GsonUtil.readValue(event_value, AfCreateOrderSuccess.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 创建订单数
                num = 1;
            }
            if(event_name.equalsIgnoreCase(EventConstants.AF_PURCHASE)){
                AfPurchase obj = GsonUtil.readValue(event_value, AfPurchase.class);
                // sku编码
                sku = obj.getAf_content_id();
                // 支付订单数
                num = 1;
            }
            if(StrUtil.isNotBlank(sku) && num > 0){
                String[] tokens = sku.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Product(app_name,platform,event_time,event_name,token, 1));
                    }
                }
            }
        }
    }
}
