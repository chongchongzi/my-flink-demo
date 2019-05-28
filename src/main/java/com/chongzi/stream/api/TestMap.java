package com.chongzi.stream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestMap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> input=env.generateSequence(0,10);

        DataStream plusOne=input.map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("--------------------"+value);
                return value+1;
            }
        });

        plusOne.print();

        env.execute();
    }
}
