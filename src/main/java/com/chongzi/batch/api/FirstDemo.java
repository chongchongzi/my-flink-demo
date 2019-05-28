package com.chongzi.batch.api;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 取前n个元素
 */
public class FirstDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<String, String, Double>> tuples1 = env.fromElements(
                new Tuple3<>("lisi","shandong",2400.00),
                new Tuple3<>("zhangsan","hainan",2600.00),
                new Tuple3<>("wangwu","shandong",2400.00),
                new Tuple3<>("zhaoliu","hainan",2600.00),
                new Tuple3<>("xiaoqi","guangdong",2400.00),
                new Tuple3<>("xiaoba","henan",2600.00)
        );


        // 取前2个元素
        DataSet<Tuple3<String, String, Double>> text2 = tuples1.first(2);
        text2.print();

        // 分组后取前2个元素
        DataSet<Tuple3<String, String, Double>> text3 = tuples1.groupBy(1).first(2);
        text3.print();

        // 分组排序后取前2个元素
        DataSet<Tuple3<String, String, Double>> text4 = tuples1.groupBy(1).sortGroup(2, Order.ASCENDING).first(3);
        text4.print();
    }
}
