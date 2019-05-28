package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * 设置DataSet的并行度，设置的并行度必须大于1
 */
public class SetParallelismDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建一个 DataSet其元素为String类型
        DataSet<String> text = env.fromElements("A", "B", "C");
        //2.设置DataSet的并行度。
        ((DataSource<String>) text).setParallelism(2);
        //3.获取DataSet的并行度。
        System.out.println(((DataSource<String>) text).getParallelism());
    }
}
