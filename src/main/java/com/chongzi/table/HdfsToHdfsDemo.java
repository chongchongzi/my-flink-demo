package com.chongzi.table;

import com.chongzi.bean.Record;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsToHdfsDemo {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsToHdfsDemo.class);
    public static void main(String[] args) throws Exception {
        LOG.info("This message contains {} placeholders. {}", 2, "chongzi");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<Record> csvInput = env
                .readCsvFile("D://projects//my-flink-demo//src//main//resources//data//olympic-athletes.csv")
                .pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
        Table atheltes = tableEnv.fromDataSet(csvInput);
        tableEnv.registerTable("athletes", atheltes);
        Table groupedByCountry = tableEnv.sqlQuery("SELECT country, SUM(total) as frequency FROM athletes WHERE country = 'a' group by country");
        //DataSet<Result> result = tableEnv.toDataSet(groupedByCountry, Result.class);
        TableSink csvSink = new CsvTableSink("D://projects//my-flink-demo//src//main//resources//data//result.csv",",",1, FileSystem.WriteMode.NO_OVERWRITE);
        groupedByCountry.writeToSink(csvSink);
       // result.print();
        env.execute("realtime-recommendation-product-detail-date-statistics");
    }

    public static class Result {
        public String country;
        public Integer frequency;

        public Result() {
            super();
        }

        public Result(String country, Integer total) {
            this.country = country;
            this.frequency = total;
        }

        @Override
        public String toString() {
            return "Result " + country + " " + frequency;
        }
    }

}
