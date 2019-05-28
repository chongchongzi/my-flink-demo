package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class IncrementingNumbers {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Integer maxIterations=10;
        // Create initialIterativeDataSet
        DataSet<Long> input = env.generateSequence(1,5);

        //启动迭代
        IterativeDataSet initial =input.iterate(maxIterations);

        //指定step fun
        DataSet<Long> iteration = initial.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long i) throws Exception {
                return i + 1;
            }
        });

        // Iteratively transform the IterativeDataSet
        DataSet<Long> result = initial.closeWith(iteration);

        //后续处理
        //result

        //结果保存
        result.print();

        // execute program
        //env.execute("IterativePi Example");
    }
}
