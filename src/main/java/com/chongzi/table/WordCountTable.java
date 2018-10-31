package com.chongzi.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * 演示在Java中使用表API来计算单词计数的简单示例。
 *
 * <p>
 * 这个例子展示了如何：
 *
 * 将数据集转换为表
 *
 * 应用组、聚合、选择和筛选操作
 */
public class WordCountTable {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Ciao", 1),
				new WC("Hello", 1));

		Table table = tEnv.fromDataSet(input);

		Table filtered = table
				.groupBy("word")
				.select("word, frequency.sum as frequency")
				.filter("frequency = 2");

		DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
		result.writeAsText("D://projects//my-flink-demo//src//main//resources//data//result.csv").setParallelism(3);
		result.print();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}
}
