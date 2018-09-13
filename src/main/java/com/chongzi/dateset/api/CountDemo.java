package com.chongzi.dateset.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 计算DataSet中元素的个数
 */
public class CountDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建一个 DataSet其元素为String类型
        DataSet<String> text = env.fromElements("A", "B", "C");
        //2.计算DataSet中元素的个数。
        long text2 = text.count();

        System.out.println(text2);
    }
}
