package com.chongzi.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * @Description 在OrcTableSource读取ORC文件。ORC是结构化数据的文件格式，并以压缩的柱状表示形式存储数据。ORC非常高效，支持Projection和滤波器下推
 * @Author chongzi
 * @Date 2018/11/6 9:03
 **/
public class OrcDemo {
    private static final String TEST_FILE_FLAT = "D://projects//my-flink-demo//src//main//resources//data//test-data-flat.orc";
    private static final String TEST_SCHEMA_FLAT =
            "struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        /*Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
        String filePath = "hdfs://127.0.0.1:8020/orc/test.orc";
         OrcTableSource orc = OrcTableSource.builder()
                .path(filePath)
                .forOrcSchema(TEST_SCHEMA_FLAT)
                .withConfiguration(config)
                .build();
        */
        OrcTableSource orc = OrcTableSource.builder()
                .path(TEST_FILE_FLAT)
                .forOrcSchema(TEST_SCHEMA_FLAT)
                .build();
        tEnv.registerTableSource("OrcTable", orc);

        String query =
                "SELECT COUNT(*), " +
                        "MIN(_col0), MAX(_col0), " +
                        "MIN(_col1), MAX(_col1), " +
                        "MIN(_col2), MAX(_col2), " +
                        "MIN(_col3), MAX(_col3), " +
                        "MIN(_col4), MAX(_col4), " +
                        "MIN(_col5), MAX(_col5), " +
                        "MIN(_col6), MAX(_col6), " +
                        "MIN(_col7), MAX(_col7), " +
                        "MIN(_col8), MAX(_col8) " +
                        "FROM OrcTable";
        Table t = tEnv.sqlQuery(query);

        DataSet<Row> dataSet = tEnv.toDataSet(t, Row.class);
        List<Row> result = dataSet.collect();
        System.out.println(result.size());
        System.out.println(result.get(0).toString());
    }


}
