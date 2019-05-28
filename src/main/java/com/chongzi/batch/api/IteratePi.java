package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class IteratePi {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Integer maxIterations=10000;
        // Create initialIterativeDataSet
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(maxIterations);

        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();

                return i + ((x * x + y * y <= 1) ? 1 : 0);
            }
        });

        // Iteratively transform the IterativeDataSet
        //count是落在扇形里的次数
        DataSet<Integer> count = initial.closeWith(iteration);

//        count.print();

        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer count) throws Exception {
                return count / (double) maxIterations * 4;
            }
        }).print();

        // execute theprogram
        //env.execute("IterativePi Example");
    }
}
