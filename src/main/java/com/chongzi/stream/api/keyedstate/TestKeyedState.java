package com.chongzi.stream.api.keyedstate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestKeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long,Long>> inputStream=env.fromElements(
                Tuple2.of(1L,4L),
                Tuple2.of(2L,3L),
                Tuple2.of(3L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(3L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(2L,2L),
                Tuple2.of(2L,9L)
        );

        inputStream
                .keyBy(0)
                .flatMap(new CountWithKeyedState())
                .setParallelism(10)
                .print();

        env.execute();
    }
}
