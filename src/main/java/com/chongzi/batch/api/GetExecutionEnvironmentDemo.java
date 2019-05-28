package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 获取DataSet的执行环境上下文,这个跟上下文和当前的DataSet有关，不是全局的
 */
public class GetExecutionEnvironmentDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建一个 DataSet其元素为String类型
        DataSet<String> text = env.fromElements("A", "B", "C");
        DataSet<String> text2 = env.fromElements("A", "B");
        System.out.println(env);
        System.out.println(text.getExecutionEnvironment());
        System.out.println(text2.getExecutionEnvironment());
        System.out.println(text.getExecutionEnvironment()==text2.getExecutionEnvironment());
    }
}
