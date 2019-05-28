package com.chongzi.stream.api.operatorstate;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TestOperatorState {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig=env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
//        StateBackend backend=new FsStateBackend(
//                "hdfs://namenode:40010/flink/checkpoints",
//                false);

//        StateBackend backend=new MemoryStateBackend(10*1024*1024,false);

        StateBackend backend=new RocksDBStateBackend(
                "hdfs://namenode:40010/flink/checkpoints",
                true);

        env.setStateBackend(backend);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        DataStream<Long> inputStream=env.fromElements(1L,2L,3L,4L,5L,1L,3L,4L,5L,6L,7L,1L,4L,5L,3L,9L,9L,2L,1L);

        inputStream.flatMap(new CountWithOperatorState())
                .setParallelism(1)
                .print();

        env.execute();

    }
}
