package com.chongzi.stream.redis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @Description redis示例
 * @Author chongzi
 * @Date 2018/11/7 9:46
 **/
public class RedisDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements("To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        DataStream<Tuple2<String, Integer>> stream =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6377).build();
        stream.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisExampleMapper()));
        /*Jedis jedis=new Jedis("127.0.0.1",6377);
        System.out.println("Server is running: " + jedis.ping());
        System.out.println("result:"+jedis.hgetAll("flink"));*/
        env.execute("RedisDemo");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
