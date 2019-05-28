package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 在分组后的数据中，获取每组最大的元素
 */
public class MaxByDemo {
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
        //2.以name进行分组，获取age最小的元素
        DataSet<Tuple3<Integer,String, Double>> input2  = input.groupBy(1).maxBy(0);
        input2.print();

        //2..以name进行分组，获取height和age最小的元素
        DataSet<Tuple3<Integer,String, Double>> input3  = input.groupBy(1).maxBy(2,0);
        input3.print();

    }
}
