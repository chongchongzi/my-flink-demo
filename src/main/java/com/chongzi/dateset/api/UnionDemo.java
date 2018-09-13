package com.chongzi.dateset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 合并多个DataSet
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<String, String, Double>> tuples1 = env.fromElements(
                new Tuple3<>("lisi-1","shandong",2400.00),
                new Tuple3<>("zhangsan-1","henan",2600.00)
        );
        DataSet<Tuple3<String, String, Double>> tuples2 = env.fromElements(
                new Tuple3<>("lisi-2","shandong",2400.00),
                new Tuple3<>("zhangsan-2","henan",2600.00)
        );
        DataSet<Tuple3<String, String, Double>> tuples3 = env.fromElements(
                new Tuple3<>("lisi-3","shandong",2400.00),
                new Tuple3<>("zhangsan-3","henan",2600.00)
        );

        // 2.合并操作
        DataSet<Tuple3<String, String, Double>> text2 = tuples1.union(tuples2).union(tuples3);

        //3.显示结果
        text2.print();
    }
}
