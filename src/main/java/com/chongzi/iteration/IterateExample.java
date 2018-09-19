/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chongzi.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 示例说明FLink流中的迭代
 * <p> 程序归纳随机数并计数加法
 * 它以迭代的流式方式达到特定的阈值. </p>
 *
 * <p>
 * 这个例子展示了如何使用:
 * <ul>
 *   <li>流迭代,
 *   <li>缓冲超时以提高等待时间,
 *   <li>定向输出.
 *   <li>Split：根据一些标准将流分成两个或更多个流。.
 *   <li>Select：在一个SplitStream上选择一个或多个流.
 * </ul>
 * </p>
 */
public class IterateExample {

	private static final int BOUND = 100;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// 检查输入参数
		final ParameterTool params = ParameterTool.fromArgs(args);

		// 设置整数对流的输入

		// 获取执行环境并将setBufferTimeout设置为1启用
		// 连续刷新输出缓冲器（最低等待时间）
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setBufferTimeout(1);

		// 在Web界面中提供参数
		env.getConfig().setGlobalJobParameters(params);

		// 通过source函数创建初始的流对象inputStream
		DataStream<Tuple2<Integer, Integer>> inputStream;
		if (params.has("input")) {
			inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
		} else {
			System.out.println("使用默认输入数据集执行迭代示例");
			System.out.println("使用-输入指定文件输入");
			inputStream = env.addSource(new RandomFibonacciSource());
		}

		// 为了对新计算的斐波那契数列中的值以及累加的迭代次数进行存储，我们须要将二元组数据流转换为五元组数据流，并据此创建迭代对象
		IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream.map(new InputMap())
				.iterate(5000);
		//注意上面代码段中iterate API的參数5000，不是指迭代5000次，而是等待反馈输入的最大时间间隔为5秒.
		//流被觉得是无界的。所以无法像批处理迭代那样指定最大迭代次数。但它同意指定一个最大等待间隔，假设在给定的时间间隔里没有元素到来。那么将会终止迭代。

		// Step函数得到下一个斐波那契数
		// 递增计数器并用输出选择器分离输出
		SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
				.split(new MySelector());

		// 对需要再次迭代的，就通过迭代流的closeWith方法反馈给迭代头
		it.closeWith(step.select("iterate"));

		// 而对于不需要的迭代就直接让其流向下游处理，这里我们仅仅是简单得将流“重构”了一下然后直接输出
		DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());

		// 发射结果
		if (params.has("output")) {
			numbers.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			numbers.print();
		}

		// 执行程序
		env.execute("Streaming Iteration Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * 该source函数会生成二元组序列，二元组的两个字段值是随机生成的作为斐波那契数列的初始值
	 */
	private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		private Random rnd = new Random();

		private volatile boolean isRunning = true;
		private int counter = 0;

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

			while (isRunning && counter < BOUND) {
				int first = rnd.nextInt(BOUND / 2 - 1) + 1;
				int second = rnd.nextInt(BOUND / 2 - 1) + 1;

				ctx.collect(new Tuple2<>(first, second));
				counter++;
				Thread.sleep(50L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	/**
	 * Generate random integer pairs from the range from 0 to BOUND/2.
	 */
	private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(String value) throws Exception {
			String record = value.substring(1, value.length() - 1);
			String[] splitted = record.split(",");
			return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
		}
	}

	/**
	 * 映射输入，以便可以在保留原始输入元组的同时计算下一个斐波那契数。
	 * 计数器连接到元组，并在每次迭代步骤中递增。
	 * 以下五元组中，当中索引为0。1这两个位置的元素，始终都是最初生成的两个元素不会变化，而后三个字段都会随着迭代而变化。
	 */
	public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
			Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws
				Exception {
			return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
		}
	}

	/**
	 * 迭代步长函数，计算下一个斐波那契数。
	 * 后三个字段会产生变化。在计算之前，数列最后一个元素会被保留。也就是f3相应的元素，然后通过f2元素加上f3元素会产生最新值并更新f3元素。而f4则会累加。
	 * 随着迭代次数添加，不是整个数列都会被保留。仅仅有最初的两个元素和最新的两个元素会被保留，这里也不是必需保留整个数列，由于我们不须要完整的数列。我们仅仅须要对最新的两个元素进行推断就可以。
	 */
	public static class Step implements
			MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
					Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
				Integer> value) throws Exception {
			return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
		}
	}

	/**
	 * 上文我们对每一个元素计算斐波那契数列的新值并产生了fibonacciStream，可是我们须要对最新的两个值进行推断。看它们是否超过了指定的阈值。超过了阈值的元组将会被输出，而没有超过的则会再次參与迭代。因此这将产生两个不同的分支。我们也为此构建了分支流：
	 * 在筛选方法select中，我们对不同的分支以不同的常量标识符进行标识：iterate（还要继续迭代）output（直接输出）。
	 * 产生了分支流之后。我们就能够从中检出不同的流分支做迭代或者输出处理。
	 */
	public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
			List<String> output = new ArrayList<>();
			if (value.f2 < BOUND && value.f3 < BOUND) {
				output.add("iterate");
			} else {
				output.add("output");
			}
			return output;
		}
	}

	/**
	 * 所谓的重构就是将之前的五元组又一次缩减为三元组
	 */
	public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
			Tuple2<Tuple2<Integer, Integer>, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer>
				value) throws
				Exception {
			return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
		}
	}

}
