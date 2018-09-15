package com.chongzi.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 本例中，Kafka消息格式固定为：时间戳,主机名,当前可用内存数。其中主机名固定设置为machine-1，而时间戳和当前可用内存数都是动态获取。由于本例只会启动一个Kafka producer来模拟单台机器发来的消息，因此在最终的统计结果中只会统计machine-1这一台机器的内存
 * 将获取到的每条Kafka消息根据“，”分割取出其中的主机名和内存数信息
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }
}