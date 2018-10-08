package com.chongzi.kafka;

import cn.hutool.core.util.StrUtil;
import com.chongzi.log.util.AppLogConvertUtil;
import com.chongzi.log.util.GsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @Description 实时推荐数据分析-商品维度
 * @Author chongzi
 * @Date 2018/10/7 9:44
 **/
public class KafkaToKafkaDemo {
    static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaToKafkaDemo.class);

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties properties = new Properties();
        String bootstrapServers = parameterTool.getRequired("bootstrap.servers");
        String groupId = parameterTool.getRequired("group.id");
        String topic = parameterTool.getRequired("topic");
        String topicId = parameterTool.getRequired("topic-id");
        if(StrUtil.isBlank(bootstrapServers)){
            bootstrapServers = "127.0.0.1:9092,127.0.0.1:9092,127.0.0.1:9092";
        }
        if(StrUtil.isBlank(groupId)){
            groupId = "log_flink_convert_test";
        }
        if(StrUtil.isBlank(topic)){
            topic = "glbg-analitic";
        }
        if(StrUtil.isBlank(topicId)){
            topic = "glbg-analitic-json-pc";
        }
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);

        DataStream<String> messageStream = see.addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties));

        DataStream<String> stringDataStream = messageStream.filter(s -> s.contains("/_app.gif?")).map(new MapFunction<String, String>() {

            /**
             * The mapping method. Takes an element from the input data set and transforms
             * it into exactly one element.
             *
             * @param value The input value.
             * @return The transformed value
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
             *                   to fail and may trigger recovery.
             */
            @Override
            public String map(String value) throws Exception {
                try {
                    return GsonUtil.toJson(AppLogConvertUtil.getAppLogParameters(value));
                } catch (Exception e) {
                    logger.error("kafka 消息格式转换出错, msg: {}", value, e);
                }
                return null;
            }
        });


        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<>(bootstrapServers, topicId,
                new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);

        stringDataStream.addSink(myProducer);

        see.execute("nginx_app_log_convert");

    }

}
