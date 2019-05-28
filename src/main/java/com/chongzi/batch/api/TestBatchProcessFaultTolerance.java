package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TestBatchProcessFaultTolerance {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, // number of restart attempts
//                Time.of(10, TimeUnit.SECONDS) // delay
//        ));

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                2,
                Time.of(1, TimeUnit.HOURS),
                Time.of(10, TimeUnit.SECONDS)
        ));

        DataSet<String> inputs=env.fromElements(
                "1",
                "2",
                "3",
                "",
                "4"
        );

        inputs.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        }).print();

    }
}
