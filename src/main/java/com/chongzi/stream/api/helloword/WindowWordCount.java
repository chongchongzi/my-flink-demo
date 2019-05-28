package com.chongzi.stream.api.helloword;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WindowWordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final int windowSize = params.getInt("window", 10);
		final int slideSize = params.getInt("slide", 5);

		DataStream<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.flatMap(new WordCount.Tokenizer())
				// create windows of windowSize records slided every slideSize records
				.keyBy(0)
				.countWindow(windowSize, slideSize)
				// group by the tuple field "0" and sum up tuple field "1"
				.sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("WindowWordCount");
	}
}