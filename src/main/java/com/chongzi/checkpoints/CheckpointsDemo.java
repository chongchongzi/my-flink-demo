package com.chongzi.checkpoints;

import com.chongzi.bean.Record;
import com.chongzi.table.WordCountSQL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;


public class CheckpointsDemo {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointsDemo.class);
    public static void main(String[] args) throws Exception {
        //获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        /*env.setStateBackend(new FsStateBackend("file:///checkpoints"));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(5000);*/

        // 生成1000万的测试数据
       String[] str = new String[40000000];
        for (int i = 0; i < 40000000; i++) {
            str[i] = i+"";
        }
        DataStream<String> dataStream = env.fromElements(str);
        Table table = tableEnv.fromDataStream(dataStream);
        tableEnv.registerTable("str", table);
        Table sql = tableEnv.sqlQuery("SELECT * FROM str");
        TableSink csvSink = new CsvTableSink("D://projects//my-flink-demo//src//main//resources//data//checkpoint.csv",",",1, FileSystem.WriteMode.NO_OVERWRITE);
        sql.writeToSink(csvSink);


       /* DataStream<String> dataStream = env.readTextFile("D://projects//my-flink-demo//src//main//resources//data//checkpoint.csv").uid("source-id");
        DataStream<String> text2 = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                Integer i = Integer.parseInt(s) + 1;
                return i.toString();
            }
        }).uid("mapper-id");
        Table table = tableEnv.fromDataStream(text2);
        tableEnv.registerTable("str", table);
        Table sql = tableEnv.sqlQuery("SELECT * FROM str");
        TableSink csvSink = new CsvTableSink("D://projects//my-flink-demo//src//main//resources//data//savepoint.csv",",",1, FileSystem.WriteMode.NO_OVERWRITE);
        sql.writeToSink(csvSink);*/
        env.execute("CheckpointsDemo");
    }
}
