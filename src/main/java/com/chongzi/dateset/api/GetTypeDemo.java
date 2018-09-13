package com.chongzi.dateset.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 获取DataSet的元素的类型信息
 */
public class GetTypeDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建一个 DataSet其元素为String类型
        DataSet<String> text = env.fromElements("A", "B", "C");
        //2.获取DataSet的元素的类型信息
        System.out.println(text.getType());
    }
}
