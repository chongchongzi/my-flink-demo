package com.chongzi.dateset.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 从元组中选择一部分字段子集
 */
public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        // 设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建DataSet
        DataSet<Tuple3<Integer,String, Double>> input = env.fromElements(new Tuple3(16,"zhangasn",194.5),
                new Tuple3(17,"zhangasn",184.5),
                new Tuple3(18,"zhangasn",174.5),
                new Tuple3(16,"lisi",194.5),
                new Tuple3(17,"lisi",184.5),
                new Tuple3(18,"lisi",174.5));
        // 获取下标1和0的元组
        DataSet<Tuple2<String, Integer>> input2  = input.project(1,0);
        input2.print();
    }
}
