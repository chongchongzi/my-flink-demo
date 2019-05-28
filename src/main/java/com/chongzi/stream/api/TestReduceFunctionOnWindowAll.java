package com.chongzi.stream.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestReduceFunctionOnWindowAll {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String,String,Integer>> input=env.fromElements(ENGLISH_TRANSCRIPT);

        //求英语总分
        DataStream<Tuple3<String,String,Integer>> totalPoints=input.keyBy(0).countWindow(2).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                return new Tuple3<>(value1.f0, value1.f1 ,value1.f2+value2.f2);
            }
        });
        totalPoints.print();

        env.execute();
    }

    /**
     * 语文成绩
     */
    public static final Tuple3[] ENGLISH_TRANSCRIPT = new Tuple3[] {
            Tuple3.of("class1","张三",100),
            Tuple3.of("class1","李四",78),
            Tuple3.of("class1","王五",99),
            Tuple3.of("class2","赵六",81),
            Tuple3.of("class2","钱七",59),
            Tuple3.of("class2","马二",97)
    };
}
