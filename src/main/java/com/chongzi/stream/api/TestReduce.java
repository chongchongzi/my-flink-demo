package com.chongzi.stream.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestReduce {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<String,String,String,Integer>> input=env.fromElements(TRANSCRIPT);

        KeyedStream<Tuple4<String,String,String,Integer>, Tuple> keyedStream = input.keyBy(0);

        keyedStream.reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1, Tuple4<String, String, String, Integer> value2) throws Exception {
                value1.f3+=value2.f3;
                return value1;
            }
        }).print();

        env.execute();
    }


    public static final Tuple4[] TRANSCRIPT = new Tuple4[] {
            Tuple4.of("class1","张三","语文",100),
            Tuple4.of("class1","李四","语文",78),
            Tuple4.of("class1","王五","语文",99),
            Tuple4.of("class2","赵六","语文",81),
            Tuple4.of("class2","钱七","语文",59),
            Tuple4.of("class2","马二","语文",97)
    };
}
