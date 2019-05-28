package com.chongzi.stream.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestFlatmap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input=env.fromElements(WORDS);

        DataStream<String> wordStream=input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                String[] tokens = value.toLowerCase().split("\\W+");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(token);
                    }
                }
            }
        });

        wordStream.print();

        env.execute();
    }

    public static final String[] WORDS = new String[] {
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "And by opposing end them?--To die,--to sleep,--",
            "Be all my sins remember'd."
    };
}
