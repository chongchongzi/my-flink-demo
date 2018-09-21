package com.chongzi.hdfs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * hdfs读取和写入
 */
public class WordCountHdfs {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<Record> csvInput = env
                .readCsvFile("hdfs:///input/flink/olympic-athletes.csv")
                .pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");

        Table atheltes = tableEnv.fromDataSet(csvInput);

        tableEnv.registerTable("athletes", atheltes);

        Table groupedByCountry = tableEnv.sqlQuery("SELECT country, SUM(total) as frequency FROM athletes group by country");

        DataSet<Result> result = tableEnv.toDataSet(groupedByCountry, WordCountHdfs.Result.class);
        result.writeAsText("hdfs:///output/flink/olympic-athletes.txt");

        Table groupedByGame = atheltes.groupBy("game").select("game, total.sum as frequency");

        DataSet<GameResult> gameResult = tableEnv.toDataSet(groupedByGame, WordCountHdfs.GameResult.class);

        gameResult.writeAsText("hdfs:///output/flink/olympic-athletes2.txt");

        env.execute();

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

    public static class GameResult {
        public String game;
        public Integer frequency;

        public GameResult(String game, Integer frequency) {
            super();
            this.game = game;
            this.frequency = frequency;
        }

        public GameResult() {
            super();
        }

        @Override
        public String toString() {
            return "GameResult [game=" + game + ", frequency=" + frequency + "]";
        }

    }
}
