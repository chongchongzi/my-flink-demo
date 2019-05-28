package com.chongzi.batch.helloword;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class WordCount {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		//1、获取命令行参数
		final ParameterTool params = ParameterTool.fromArgs(args);

		//2、 set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			// get default test text data
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			text = WordCountData.getDefaultTextLineDataSet(env);
		}


		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsCsv(params.get("output"), "\n", " ");
			// execute program
			JobExecutionResult jobExecutionResult =env.execute("WordCount Example");
			jobExecutionResult.getAccumulatorResult("num-lines").toString();

		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
		private IntCounter numLines = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator("num-lines", this.numLines);
			super.open(parameters);
		}

		@Override
		public void close() throws Exception {
			super.close();
		}

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			this.numLines.add(1);
			// normalize and split the line
			//"To be, or not to be,--that is the question:--",
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}