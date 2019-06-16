package com.chongzi.stream.logMonitor;

import com.chongzi.stream.logMonitor.model.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * @Description 实时日志滑动窗口监控
 * @Author chongzi
 * @Date 2019/6/13 17:07
 * @Param 
 * @return 
 **/
@Slf4j
public class LogTimeWindowMonitor {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		// 定义源数据流
		env
			.addSource(new SimpleSourceFunction())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEvent>() {
				@Override
				public long extractAscendingTimestamp(LogEvent element) {
					return element.eventTime;
				}
			})
				.filter(new FilterFunction<LogEvent>() {
					@Override
					public boolean filter(LogEvent optLog) throws Exception {
						// 过滤出只有点击的数据
						return optLog.log.contains("\"-\" \"-\" 2.");
					}
				})
				.keyBy(new KeySelector<LogEvent, String>() {
				@Override
				public String getKey(LogEvent value) throws Exception {
					return value.applicationName;
				}
			})
			.timeWindow(Time.seconds(10), Time.seconds(1))
			.aggregate(new CountAgg(), new WindowResultFunction())
			.keyBy("windowEnd")
			.process(new CountWithTimeoutFunction(5))
			.print();

		env.execute("LogTimeWindowMonitor");
	}

	/** 用于输出窗口的结果 */
	public static class WindowResultFunction implements WindowFunction<Long, LogCount, String, TimeWindow> {

		@Override
		public void apply(
				String key,  // 窗口的主键，即 logName
				TimeWindow window,  // 窗口
				Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
				Collector<LogCount> collector  // 输出类型为 LogCount
		) throws Exception {
			String logName = key;
			Long count = aggregateResult.iterator().next();
			collector.collect(LogCount.of(logName, window.getEnd(), count));
		}
	}

	/** COUNT 统计的聚合函数实现，每出现一条记录加一 */
	public static class CountAgg implements AggregateFunction<LogEvent, Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(LogEvent optLog, Long acc) {
			return acc + 1;
		}

		@Override
		public Long getResult(Long acc) {
			return acc;
		}

		@Override
		public Long merge(Long acc1, Long acc2) {
			return acc1 + acc2;
		}
	}

	/** 商品点击量(窗口操作的输出类型) */
	public static class LogCount {
		public String applicationName;     // 日志名称
		public long windowEnd;  // 窗口结束时间戳
		public long logCount;  // 日志数量

		public static LogCount of(String applicationName, long windowEnd, long logCount) {
			LogCount result = new LogCount();
			result.applicationName = applicationName;
			result.windowEnd = windowEnd;
			result.logCount = logCount;
			return result;
		}

		@Override
		public String toString() {
			return "LogCount{" +
					"applicationName='" + applicationName + '\'' +
					", windowEnd=" +timeStamp2Date(windowEnd,"yyyy-MM-dd HH:mm:ss")  +
					", logCount=" + logCount +
					'}';
		}
	}

	public static class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, LogCount, String> {

		private final int topSize;

		public CountWithTimeoutFunction(int topSize) {
			this.topSize = topSize;
		}

		// 用于存储日志事件与数量的状态，待收齐同一个窗口的数据后，再触发告警计算
		private ListState<LogCount> logCountState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<LogCount> itemsStateDesc = new ListStateDescriptor<>(
					"logCount-state",
					LogCount.class);
			logCountState = getRuntimeContext().getListState(itemsStateDesc);
		}

		@Override
		public void processElement(
				LogCount input,
				Context context,
				Collector<String> collector) throws Exception {

			// 每条数据都保存到状态中
			logCountState.add(input);
			// 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
			context.timerService().registerEventTimeTimer(input.windowEnd + 1);
		}

		@Override
		public void onTimer(
				long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// 获取收到的所有商品点击量
			List<LogCount> allItems = new ArrayList<>();
			for (LogCount item : logCountState.get()) {
				allItems.add(item);
			}
			// 提前清除状态中的数据，释放空间
			logCountState.clear();
			// 按照点击量从大到小排序
			allItems.sort(new Comparator<LogCount>() {
				@Override
				public int compare(LogCount o1, LogCount o2) {
					return (int) (o2.logCount - o1.logCount);
				}
			});
			// 将排名信息格式化成 String, 便于打印
			StringBuilder result = new StringBuilder();
			result.append("====================================\n");
			result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");

			for (int i=0; i<allItems.size(); i++) {
				LogCount currentItem = allItems.get(i);
				// No1:  商品ID=12224  浏览量=2413
				if(currentItem.logCount >= topSize){
					result.append("No").append(i).append(":")
							.append("  应用名称=").append(currentItem.applicationName)
							.append("  数量=").append(currentItem.logCount)
							.append("  超出阀值=").append(topSize)
							.append("\n");
				}else{
					result.append("No").append(i).append(":")
							.append("  应用名称=").append(currentItem.applicationName)
							.append("  数量=").append(currentItem.logCount)
							.append("\n");
				}
			}
			result.append("====================================\n\n");

			// 控制输出频率，模拟实时滚动结果
			//Thread.sleep(1000);

			out.collect(result.toString());
		}
	}

	public static final String[] nameArray = new String[] {
			"响应超时",
			"结果为空",
			"服务异常"
	};

	private static class SimpleSourceFunction implements SourceFunction<LogEvent> {
		private long num = 0L;
		private volatile boolean isRunning = true;
		@Override
		public void run(SourceContext<LogEvent> sourceContext) throws Exception {

			while (isRunning) {
				int randomNum=(int)(1+Math.random()*(3-1+1));
				double a = Math.random()*(3-1)+1;
				String log = "172.31.62.243 - - [27/May/2019:14:32:21 +0000] \"POST / HTTP/1.1\" 200 200 \"page_file:\"  12104 \"recommendType=2010102&params={\\x22lang\\x22:\\x22en\\x22,\\x22pipelinecode\\x22:\\x22ZFAU\\x22,\\x22uctype\\x22:\\x22N\\x22,\\x22regioncode\\x22:\\x22AU\\x22,\\x22platform\\x22:\\x22IOS\\x22,\\x22cookie\\x22:\\x221558368574502-7978181\\x22,\\x22pageindex\\x22:0,\\x22pagesize\\x22:18,\\x22ip\\x22:\\x2260.228.50.35\\x22,\\x22ortherparams\\x22:{\\x22gender\\x22:\\x22\\x22,\\x22birthday\\x22:\\x22\\x22,\\x22adgroup\\x22:\\x22\\x22},\\x22policy\\x22:0}\" \"-\" \"-\" "+a+" 2.014 glbgcl.logsss.com";
				sourceContext.collect(LogEvent.of(nameArray[randomNum-1],System.currentTimeMillis(),log));
				num++;
				Thread.sleep(100);
			}
		}
		@Override
		public void cancel() {
			isRunning = false;
		}

	}

	/**
	 * 时间戳转换成日期格式字符串
	 * @param seconds 精确到秒的字符串
	 * @param format
	 * @return
	 */
	public static String timeStamp2Date(long seconds,String format) {
		if(format == null || format.isEmpty()){
			format = "yyyy-MM-dd HH:mm:ss";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(seconds));
	}
}