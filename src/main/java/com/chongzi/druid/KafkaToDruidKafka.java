package com.chongzi.druid;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.chongzi.bean.ProductDetail;
import com.chongzi.event.AfContentId;
import com.chongzi.event.AfCreateOrderSuccess;
import com.chongzi.event.AfPurchase;
import com.chongzi.kafka.EventConstants;
import com.chongzi.log.util.AppLogConvertUtil;
import com.chongzi.log.util.GsonUtil;
import com.chongzi.log.util.SiteUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @Description 实时推荐数据分析-商品维度 明细维度从kafka写入hdfs
 * @Author chongzi
 * @Date 2018/10/7 9:44
 **/
public class KafkaToDruidKafka {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToDruidKafka.class);

    public static void main(String args[]) throws Exception {
        final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);
        ParameterTool params = ParameterTool.fromArgs(args);
        //String bootstrapServers = "172.31.35.194:9092,127.31.50.250:9092,172.31.63.112:9092";
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "realtime-product-2018102601";
        String topic = "glbg-analitic";
        String producerTopic = "mycount";
        if (params.has("bootstrapServers")) {
            bootstrapServers = params.get("bootstrapServers");
        }
        if (params.has("groupId")) {
            groupId = params.get("groupId");
        }
        if (params.has("topic")) {
            topic = params.get("topic");
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);
        //从头读取
        properties.setProperty("auto.offset.reset","earliest");

        //DataStream<String> messageStream = see.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties));
        DataStream<String> messageStream = see.fromElements("127.0.0.1^A^-^A^[08/Oct/2018:07:04:23 +0000]^A^\"GET /_app.gif?re_targeting_conversion_type=&is_retargeting=false&app_id=com.zaful&platform=android&event_type=in-app-event&attribution_type=organic&event_time=2018-09-08%2007%3A04%3A22&event_time_selected_timezone=2018-10-08%2015%3A04%3A22.509%2B0800&event_name=af_impression&event_value=%7B%22af_content_id%22%3A%22226385802%2C276932601%2C279767801%2C278595702%2C233930401%2C279781303%2C226386103%2C279846802%2C236254603%2C226385102%2C256051301%2C257337701%22%2C%22af_inner_mediasource%22%3A%22recommend%20productdetail%22%2C%22af_version_id%22%3A%22234%22%2C%22af_plan_id%22%3A%2289%22%2C%22af_bucket_id%22%3A%222%22%7D&currency=USD&selected_currency=USD&revenue_in_selected_currency=&cost_per_install=&click_time=&click_time_selected_timezone=&install_time=2018-09-09%2019%3A44%3A16&install_time_selected_timezone=2018-09-10%2003%3A44%3A16.131%2B0800&agency=&media_source=Organic&campaign=&fb_campaign_id=&fb_campaign_name=&fb_adset_name=&fb_adset_id=&fb_adgroup_id=&fb_adgroup_name=&af_siteid=&country_code=IN&city=Nagpur%20City&ip=106.210.247.27&wifi=false&language=English&appsflyer_device_id=1536522249835-4170699118690066416&customer_user_id=0&android_id=&imei=&advertising_id=cbce21e9-b091-4205-85ae-0ea5bcd6e74c&mac=&device_brand=samsung&device_model=SM-G610F&os_version=6.0.1&sdk_version=v4.8.14&app_version=3.6.1&operator=airtel&carrier=airtel&af_sub1=&af_sub2=&af_sub3=&af_sub4=&af_sub5=&click_url=&http_referrer=&app_name=Zaful%20-%20Your%20Way%20to%20Save%20on%20Black%20Friday&download_time=2018-09-09%2019%3A40%3A54&download_time_selected_timezone=2018-09-10%2003%3A40%3A54.000%2B0800&af_keywords=&bundle_id=com.zaful&attributed_touch_type=&attributed_touch_time= HTTP/1.1\"^A^200^A^372^A^\"-\"^A^\"http-kit/2.0\"^A^s.logsss.com^A^34.240.29.179, 10.221.222.58, 88.221.222.146, 10.55.59.245, 23.55.59.236^A^34.240.29.179^A^US^A^United States^A^-^A^1538982263",
                "127.0.0.1^A^-^A^[08/Oct/2018:07:04:23 +0000]^A^\"GET /_app.gif?re_targeting_conversion_type=&is_retargeting=false&app_id=com.zaful&platform=android&event_type=in-app-event&attribution_type=organic&event_time=2018-08-09%2010%3A04%3A22&event_time_selected_timezone=2018-10-08%2015%3A04%3A22.509%2B0800&event_name=af_impression&event_value=%7B%22af_content_id%22%3A%22226385802%2C276932601%2C279767801%2C278595702%2C233930401%2C279781303%2C226386103%2C279846802%2C236254603%2C226385102%2C256051301%2C257337701%22%2C%22af_inner_mediasource%22%3A%22recommend%20productdetail%22%2C%22af_version_id%22%3A%22234%22%2C%22af_plan_id%22%3A%2289%22%2C%22af_bucket_id%22%3A%222%22%7D&currency=USD&selected_currency=USD&revenue_in_selected_currency=&cost_per_install=&click_time=&click_time_selected_timezone=&install_time=2018-09-09%2019%3A44%3A16&install_time_selected_timezone=2018-09-10%2003%3A44%3A16.131%2B0800&agency=&media_source=Organic&campaign=&fb_campaign_id=&fb_campaign_name=&fb_adset_name=&fb_adset_id=&fb_adgroup_id=&fb_adgroup_name=&af_siteid=&country_code=IN&city=Nagpur%20City&ip=106.210.247.27&wifi=false&language=English&appsflyer_device_id=1536522249835-4170699118690066416&customer_user_id=0&android_id=&imei=&advertising_id=cbce21e9-b091-4205-85ae-0ea5bcd6e74c&mac=&device_brand=samsung&device_model=SM-G610F&os_version=6.0.1&sdk_version=v4.8.14&app_version=3.6.1&operator=airtel&carrier=airtel&af_sub1=&af_sub2=&af_sub3=&af_sub4=&af_sub5=&click_url=&http_referrer=&app_name=Zaful%20-%20Your%20Way%20to%20Save%20on%20Black%20Friday&download_time=2018-09-09%2019%3A40%3A54&download_time_selected_timezone=2018-09-10%2003%3A40%3A54.000%2B0800&af_keywords=&bundle_id=com.zaful&attributed_touch_type=&attributed_touch_time= HTTP/1.1\"^A^200^A^372^A^\"-\"^A^\"http-kit/2.0\"^A^s.logsss.com^A^34.240.29.179, 10.221.222.58, 88.221.222.146, 10.55.59.245, 23.55.59.236^A^34.240.29.179^A^US^A^United States^A^-^A^1538982263",
                "127.0.0.1^A^-^A^[08/Oct/2018:07:04:23 +0000]^A^\"GET /_app.gif?re_targeting_conversion_type=&is_retargeting=false&app_id=com.zaful&platform=android&event_type=in-app-event&attribution_type=organic&event_time=2017-10-10%2010%3A04%3A22&event_time_selected_timezone=2018-10-08%2015%3A04%3A22.509%2B0800&event_name=af_impression&event_value=%7B%22af_content_id%22%3A%22226385802%2C276932601%2C279767801%2C278595702%2C233930401%2C279781303%2C226386103%2C279846802%2C236254603%2C226385102%2C256051301%2C257337701%22%2C%22af_inner_mediasource%22%3A%22recommend%20productdetail%22%2C%22af_version_id%22%3A%22234%22%2C%22af_plan_id%22%3A%2289%22%2C%22af_bucket_id%22%3A%222%22%7D&currency=USD&selected_currency=USD&revenue_in_selected_currency=&cost_per_install=&click_time=&click_time_selected_timezone=&install_time=2018-09-09%2019%3A44%3A16&install_time_selected_timezone=2018-09-10%2003%3A44%3A16.131%2B0800&agency=&media_source=Organic&campaign=&fb_campaign_id=&fb_campaign_name=&fb_adset_name=&fb_adset_id=&fb_adgroup_id=&fb_adgroup_name=&af_siteid=&country_code=IN&city=Nagpur%20City&ip=106.210.247.27&wifi=false&language=English&appsflyer_device_id=1536522249835-4170699118690066416&customer_user_id=0&android_id=&imei=&advertising_id=cbce21e9-b091-4205-85ae-0ea5bcd6e74c&mac=&device_brand=samsung&device_model=SM-G610F&os_version=6.0.1&sdk_version=v4.8.14&app_version=3.6.1&operator=airtel&carrier=airtel&af_sub1=&af_sub2=&af_sub3=&af_sub4=&af_sub5=&click_url=&http_referrer=&app_name=Zaful%20-%20Your%20Way%20to%20Save%20on%20Black%20Friday&download_time=2018-09-09%2019%3A40%3A54&download_time_selected_timezone=2018-09-10%2003%3A40%3A54.000%2B0800&af_keywords=&bundle_id=com.zaful&attributed_touch_type=&attributed_touch_time= HTTP/1.1\"^A^200^A^372^A^\"-\"^A^\"http-kit/2.0\"^A^s.logsss.com^A^34.240.29.179, 10.221.222.58, 88.221.222.146, 10.55.59.245, 23.55.59.236^A^34.240.29.179^A^US^A^United States^A^-^A^1538982263");
        DataStream<String> stringDataStream = messageStream
                .filter(s -> s.contains("/_app.gif?"))
                .flatMap(new Tokenizer());
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(bootstrapServers, producerTopic,
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);
        stringDataStream.addSink(myProducer);
        see.execute("kafka-to-druid-kafka");
    }

    public static final class Tokenizer implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) {
            String appName = null;
            String platform = null;
            String eventTime = null;
            String eventDate = null;
            Integer eventMonth = null;
            Integer eventYear = null;
            String eventName = null;
            String eventValue = null;
            String sku = null;
            String skus = null;
            String createOrderPrices = null;
            String createOrderQuantitys = null;
            String purchaseOrderPrices = null;
            String purchaseOrderQuantitys = null;
            String[] createOrderPriceStrArr = null;
            String[] createOrderQuantityStrArr = null;
            Double[] createOrderPriceArr = null;
            Integer[] createOrderQuantityArr = null;
            String[] purchaseOrderPriceStrArr = null;
            String[] purchaseOrderQuantityStrArr = null;
            Double[] purchaseOrderPriceArr = null;
            Integer[] purchaseOrderQuantityArr = null;
            Integer skuExpCnt = 0;//商品被曝光数量
            Integer skuHitCnt = 0;//商品被点击数量
            Integer skuCartCnt = 0;//商品被加购物车数量
            Integer skuMarkedCnt = 0;//商品被加收藏数量
            Integer createOrderCnt = 0;//创建订单总数
            Integer purchaseOrderCnt = 0;//购买订单总数
            Map<String, Object> map = AppLogConvertUtil.getAppLogParameters(value);
            if(map.get("app_name") != null){
                appName = SiteUtil.getAppSite(map.get("app_name").toString());
            }
            if(map.get("platform") != null){
                platform = map.get("platform").toString();
            }
            if(map.get("event_time") != null){
                eventTime = map.get("event_time").toString();
                eventYear = Integer.parseInt(DateUtil.format(DateUtil.parse(eventTime, DatePattern.NORM_DATE_FORMAT),"yyyy")) ;
                eventMonth = Integer.parseInt(DateUtil.format(DateUtil.parse(eventTime, DatePattern.NORM_DATE_FORMAT),"MM"));
                eventDate = DateUtil.parse(eventTime, DatePattern.NORM_DATE_FORMAT).toDateStr();
            }
            if(map.get("event_name") != null){
                eventName = map.get("event_name").toString();
            }
            if(map.get("event_value") != null){
                eventValue = map.get("event_value").toString();
            }
            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_IMPRESSION)){
                if(JSONUtil.isJsonObj(eventValue)){
                    AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = obj.getAf_content_id();
                    skuExpCnt = 1;
                }
            }
            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_VIEW_PRODUCT)){
                if(JSONUtil.isJsonObj(eventValue)){
                    AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = obj.getAf_content_id();
                    skuHitCnt = 1;
                }
            }
            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_ADD_TO_BAG)){
                if(JSONUtil.isJsonObj(eventValue)){
                    AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = obj.getAf_content_id();
                    skuCartCnt = 1;
                }
            }
            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_ADD_TO_WISHLIST)){
                if(JSONUtil.isJsonObj(eventValue)){
                    AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = obj.getAf_content_id();
                    skuMarkedCnt = 1;
                }
            }

            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_CREATE_ORDER_SUCCESS)){
                if(JSONUtil.isJsonObj(eventValue)){
                    AfCreateOrderSuccess obj = JSONUtil.toBean(eventValue, AfCreateOrderSuccess.class);
                    skus = obj.getAf_content_id();
                    if(obj.getAf_price() != null){
                        createOrderPrices = obj.getAf_price();
                    }
                    if(obj.getAf_quantity() != null){
                        createOrderQuantitys = obj.getAf_quantity();
                    }
                    createOrderCnt = 1;
                }
            }
            if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_PURCHASE)){
                if(JSONUtil.isJsonObj(eventValue)){
                    try{
                        AfPurchase obj = JSONUtil.toBean(eventValue, AfPurchase.class);
                        skus = obj.getAf_content_id();
                        if(obj.getAf_price() != null){
                            purchaseOrderPrices = obj.getAf_price();
                        }
                        if(obj.getAf_quantity() != null){
                            purchaseOrderQuantitys = obj.getAf_quantity();
                        }
                        purchaseOrderCnt = 1;
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
            if(StrUtil.isNotBlank(skus) && !skus.equalsIgnoreCase("null")){
                String[] skuArr = skus.toLowerCase().split(",");
                if(createOrderPrices != null){
                    createOrderPriceStrArr = createOrderPrices.split(",");
                    createOrderPriceArr =  Convert.toDoubleArray(createOrderPriceStrArr);
                }
                if(createOrderQuantitys != null){
                    createOrderQuantityStrArr = createOrderQuantitys.split(",");
                    createOrderQuantityArr = Convert.toIntArray(createOrderQuantityStrArr);
                }
                if(purchaseOrderPrices != null){
                    purchaseOrderPriceStrArr = purchaseOrderPrices.split(",");
                    purchaseOrderPriceArr =  Convert.toDoubleArray(purchaseOrderPriceStrArr);
                }
                if(purchaseOrderQuantitys != null){
                    purchaseOrderQuantityStrArr = purchaseOrderQuantitys.split(",");
                    purchaseOrderQuantityArr = Convert.toIntArray(purchaseOrderQuantityStrArr);
                }
                for (int i = 0; i < skuArr.length; i++) {
                    sku = skuArr[i];
                    Double gmv = 0.00;
                    Double createOrderPrice = 0.00;
                    Integer createOrderQuantity = 0;
                    Double purchaseOrderAmount = 0.00;
                    Double purchaseOrderPrice = 0.00;
                    Integer purchaseOrderQuantity = 0;
                    try {
                        if(createOrderPriceArr != null && createOrderPriceArr[i] != null){
                            createOrderPrice = createOrderPriceArr[i];
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }

                    try {
                        if(createOrderQuantityArr != null && createOrderQuantityArr[i] != null){
                            createOrderQuantity = createOrderQuantityArr[i];
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    try {
                        if(purchaseOrderPriceArr != null && purchaseOrderPriceArr[i] != null){
                            purchaseOrderPrice = purchaseOrderPriceArr[i];
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }

                    try {
                        if(purchaseOrderQuantityArr != null && purchaseOrderQuantityArr[i] != null){
                            purchaseOrderQuantity = purchaseOrderQuantityArr[i];
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    if (StrUtil.isNotBlank(sku) && !sku.equalsIgnoreCase("null")) {
                        gmv = NumberUtil.mul(createOrderPrice,createOrderQuantity).doubleValue();
                        gmv = NumberUtil.round(gmv,2).doubleValue();
                        createOrderPrice = NumberUtil.round(createOrderPrice,2).doubleValue();
                        purchaseOrderAmount = NumberUtil.mul(purchaseOrderPrice,purchaseOrderQuantity).doubleValue();
                        purchaseOrderAmount = NumberUtil.round(purchaseOrderAmount,2).doubleValue();
                        purchaseOrderPrice = NumberUtil.round(purchaseOrderPrice,2).doubleValue();
                        ProductDetail productDetail = new ProductDetail(appName,platform,eventName,sku,eventTime,eventYear,eventMonth,eventDate,createOrderPrice,createOrderQuantity,gmv,purchaseOrderPrice,purchaseOrderQuantity,purchaseOrderAmount,skuExpCnt,skuHitCnt,skuCartCnt,skuMarkedCnt,createOrderCnt,purchaseOrderCnt);
                        out.collect(GsonUtil.toJson(productDetail));
                    }
                }
            }
        }
    }
}
