package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 获取元素的累加和，只能作用于数值类型
 */
public class SumDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建DataSet[Student]
        DataSet<Tuple3<Integer,String, Double>> input = env.fromElements(new Tuple3(16,"zhangasn",194.5),
                new Tuple3(17,"zhangasn",184.5),
                new Tuple3(18,"zhangasn",174.5),
                new Tuple3(16,"lisi",194.5),
                new Tuple3(17,"lisi",184.5),
                new Tuple3(18,"lisi",174.5));
        //2.获取age汇总的元素
        DataSet<Tuple3<Integer,String, Double>> input2  = input.sum(0);
        input2.print();
    }
}
