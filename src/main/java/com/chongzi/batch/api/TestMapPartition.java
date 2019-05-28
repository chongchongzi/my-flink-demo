package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestMapPartition {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> inputs=env.generateSequence(1,20);

        inputs.mapPartition(new MyMapPartitionFunction())
                .print();
    }

    public static class MyMapPartitionFunction implements MapPartitionFunction<Long,Long> {

        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
            long c = 0;
            for (Long s : values) {
                c++;
            }
            out.collect(c);
        }
    }
}
