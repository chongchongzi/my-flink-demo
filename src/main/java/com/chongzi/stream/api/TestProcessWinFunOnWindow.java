package com.chongzi.stream.api;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class TestProcessWinFunOnWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String,String,Long>> input=env.fromElements(ENGLISH_TRANSCRIPT);

        //求各班级英语平均分
        DataStream<Double> avgScore=input.keyBy(0).countWindow(2).process(new MyProcessWindowFunction());
        avgScore.print();
        env.execute();
    }

    /**
     * 英语成绩
     */
    public static final Tuple3[] ENGLISH_TRANSCRIPT = new Tuple3[] {
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","钱七",59L),
            Tuple3.of("class2","马二",97L)
    };

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple3<String,String,Long>, Double, Tuple, GlobalWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Double> out) throws Exception {
            long sum=0;
            long count = 0;
            for (Tuple3<String, String, Long> in: elements) {
                sum+=in.f2;
                count++;
            }
            //out.collect("Window: " + context.window() + "count: " + count);
            out.collect((double) (sum/count));
        }
    }
}
