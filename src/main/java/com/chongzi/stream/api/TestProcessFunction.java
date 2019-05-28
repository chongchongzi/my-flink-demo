package com.chongzi.stream.api;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

public class TestProcessFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // the source data stream
        DataStream<OptLog> stream=env
                .addSource(new SimpleSourceFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OptLog>() {
                    @Override
                    public long extractAscendingTimestamp(OptLog element) {
                        return element.opTs;
                    }
                });

        stream.print();

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(new KeySelector<OptLog, String>() {

                    @Override
                    public String getKey(OptLog value) throws Exception {
                        return value.userName;
                    }
                })
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute();
    }

    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * 按key计数，如果某个key在30秒之内没有新的数据到来就发出(key,count)
     */
    public static class CountWithTimeoutFunction extends ProcessFunction<OptLog, Tuple2<String, Long>> {

        /** The state that is maintained by this process function */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(OptLog optLog, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = optLog.userName;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);

            // schedule the next timer 30 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + 30000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 30000) {
                // emit the state on timeout
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }

    /**
     * 操作日志
     */
    public static class OptLog{
        /**
         * 用户名
         */
        private String userName;
        /**
         * 操作类型
         */
        private int opType;
        /**
         * 时间戳
         */
        private long opTs;

        public OptLog(String userName, int opType, long opTs) {
            this.userName = userName;
            this.opType = opType;
            this.opTs = opTs;
        }

        public static OptLog of(String userName, int opType, long opTs){
            return new OptLog(userName,opType,opTs);
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public int getOpType() {
            return opType;
        }

        public void setOpType(int opType) {
            this.opType = opType;
        }

        public long getOpTs() {
            return opTs;
        }

        public void setOpTs(long opTs) {
            this.opTs = opTs;
        }

        @Override
        public String toString() {
            return "OptLog{" +
                    "userName='" + userName + '\'' +
                    ", opType=" + opType +
                    ", opTs=" + opTs +
                    '}';
        }
    }

    public static final String[] nameArray = new String[] {
            "张三",
            "李四",
            "王五",
            "赵六",
            "钱七"
    };

    private static class SimpleSourceFunction implements SourceFunction<OptLog> {
        private long num = 0L;
        private volatile boolean isRunning = true;
        @Override
        public void run(SourceContext<OptLog> sourceContext) throws Exception {
            while (isRunning) {
                int randomNum=(int)(1+Math.random()*(5-1+1));
                sourceContext.collect(OptLog.of(nameArray[randomNum-1],randomNum,System.currentTimeMillis()));
                num++;
                Thread.sleep(10000);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }

    }

}
