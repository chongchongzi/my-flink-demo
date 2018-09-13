package com.chongzi.mysql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * source测试程序
 */
public class StudentSourceFromMysqlTest {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从自定义source中读取数据
        DataStream<Student> students=env.addSource(new StudentSourceFromMysql());

        //3.显示结果
        students.print();

        //4.触发流执行
        env.execute();
    }
}