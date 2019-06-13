package com.chongzi.stream.logMonitor.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @Description 推送配置信息到kafka
 * @Author chongzi
 * @Date 2019/6/13 17:23
 * @Param
 * @return
 **/
public class LogConfigSimulator {

    public static void main(String[] args) throws Exception{

        String config="{\"applicationName\":\"王五\",\"applicationRemarks\":\"测试\",\"alarmKeywordRule\":\"\\\"-\\\" \\\"-\\\" 2.\",\"alarmRemarks\":\"推荐接口在10分钟内超时大于等于30次\",\"alarmType\":2,\"registerDate\":\"2019-05-29\",\"timeLength\":10,\"num\":3,\"email\":\"\",\"vv\":\"613032\",\"weixin\":\"\"}";

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        ProducerRecord record;
        record = new ProducerRecord<String, String>(
                "timeoutAnalysisConf",
                null,
                new Random().nextInt()+"",
                config);
        producer.send(record);
        producer.close();

    }
}
