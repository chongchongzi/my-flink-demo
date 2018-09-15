package com.chongzi.kafka;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * kafka实现Wordcount实时计算
 */
public class KafkaWordcount {
    public static void main(String[] args) throws Exception {
        //引入Flink StreamExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置监控数据流时间间隔（官方叫状态与检查点）
        env.enableCheckpointing(1000);

        //配置kafka和zookeeper的ip和端口
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");

        //将kafka和zookeeper配置信息加载到Flink StreamExecutionEnvironment
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(),
                properties);
        //将Kafka的数据转成flink的DataStream类型
        DataStream<String> stream = env.addSource(myConsumer);
        // 实施计算模型并输出结果
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

        counts.print();

        env.execute("WordCount from Kafka data");
    }

    /**
     * 计算模型具体逻辑代码
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}