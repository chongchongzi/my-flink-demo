package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.List;

/**
 * 以element为粒度，对element进行1：n的转化。
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
    // 1.设置运行环境，准备运行的数据
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<String> text = env.fromElements("flink vs spark", "buffer vs  shuffle");

    // 2.以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    DataSet<String> text2 = text.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            collector.collect(s.toUpperCase() + "--##bigdata##");
        }
    });
        text2.print();

    // 3.以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    DataSet<String[]> text3 = text.flatMap(new FlatMapFunction<String, String[]>() {
        @Override
        public void flatMap(String s, Collector<String[]> collector) throws Exception {
            collector.collect(s.toUpperCase().split("\\s+"));
        }
    });
    final List<String[]> collect = text3.collect();
    //显示结果，使用Lambda表达式的写法
        collect.forEach(arr -> {
        for (String token : arr) {
            System.out.println(token);
        }
    });
    //显示结果，不使用Lambda表达式的写法
        for (String[] arr : collect) {
        for (String token : arr) {
            System.out.println(token);
        }
    }
}
}
