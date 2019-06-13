package com.chongzi.stream.logMonitor;

import com.chongzi.stream.logMonitor.function.LogConnectedBroadcastProcessFuntion;
import com.chongzi.stream.logMonitor.model.LogConfig;
import com.chongzi.stream.logMonitor.model.LogEvaluatedResult;
import com.chongzi.stream.logMonitor.model.LogEvent;
import com.chongzi.stream.logMonitor.schema.LogConfigDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @Description 实时日志固定窗口监控
 * @Author chongzi
 * @Date 2019/5/29 14:05
 **/
@Slf4j
public class LogMonitor {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID = "group.id";
    public static final String RETRIES = "retries";
    public static final String INPUT_EVENT_TOPIC = "input-event-topic";
    public static final String INPUT_CONFIG_TOPIC = "input-config-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final MapStateDescriptor<String, LogConfig> logConfigStateDescriptor =
            new MapStateDescriptor<>(
                    "configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<LogConfig>() {}));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = parameterCheck(args);
        /**
         * 把参数显示在web页面,路径Overview-Running Jobs-Configuration
         */
        env.getConfig().setGlobalJobParameters(params);
        /**
         * 设置事件时间
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * checkpoint 60秒
         */
//        env.enableCheckpointing(60000L);
//        CheckpointConfig checkpointConf=env.getCheckpointConfig();
        /**
         * EXACTLY_ONCE:都处理一次
         */
//        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
//        checkpointConf.setCheckpointTimeout(10000L);
        //
        /**
         * 开始的时候依然保留checkpoint
         */
//        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * 生产需要开启StateBackend，checkpoint保存的路径
         */
//        env.setStateBackend(new FsStateBackend(
//                "hdfs://namenode01.td.com/flink-checkpoints/customer-purchase-behavior-tracker"));

        /**
         * restart策略,自己发生问题重启，restartAttempts：重启多少次，停多长时间重试，这里是30秒
         */
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                10, // number of restart attempts
//                org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS) // delay
//        ));

        /* Kafka consumer */
        Properties consumerProps=new Properties();
        consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID));

        // 定义源数据流
        DataStream<LogEvent> stream = env
                .addSource(new SimpleSourceFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEvent>() {
                    @Override
                    public long extractAscendingTimestamp(LogEvent element) {
                        return element.eventTime;
                    }
                });

        KeyedStream<LogEvent, String> customerLogEventStream = stream
                .keyBy(new KeySelector<LogEvent, String>() {
                    @Override
                    public String getKey(LogEvent event) throws Exception {
                        return event.applicationName;
                    }
                });

        // customerLogEventStream.print();

        //配置流
        final FlinkKafkaConsumer010 kafkaLogConfigSource = new FlinkKafkaConsumer010<LogConfig>(
                params.get(INPUT_CONFIG_TOPIC),
                new LogConfigDeserializationSchema(), consumerProps);

        final BroadcastStream<LogConfig> logConfigBroadcastStream = env
                .addSource(kafkaLogConfigSource)
                .broadcast(logConfigStateDescriptor);// 广播配置流

        //连接两个流
        DataStream<LogEvaluatedResult> connectedStream = customerLogEventStream
                .connect(logConfigBroadcastStream)
                .process(new LogConnectedBroadcastProcessFuntion());

        connectedStream.print();

        /* Kafka consumer */
//        Properties producerProps=new Properties();
//        producerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
//        producerProps.setProperty(RETRIES, "3");
//
//        final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010<EvaluatedResult>(
//                params.get(OUTPUT_TOPIC),
//                new EvaluatedResultSerializationSchema(),
//                producerProps);
//
//        /* at_ least_once 设置 */
//        kafkaProducer.setLogFailuresOnly(false);
//        kafkaProducer.setFlushOnCheckpoint(true);
//
//        connectedStream.addSink(kafkaProducer);

        env.execute("LogMonitor");

    }

    /**
     * 指定流里面的事件时间戳是什么字段，和env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);对应上
     */
    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<LogEvent> {

        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(LogEvent element) {
            return element.getEventTime();
        }

    }

    public static final String[] nameArray = new String[] {
            "张三",
            "李四",
            "王五",
            "赵六",
            "钱七"
    };

    private static class SimpleSourceFunction implements SourceFunction<LogEvent> {
        private long num = 0L;
        private volatile boolean isRunning = true;
        @Override
        public void run(SourceContext<LogEvent> sourceContext) throws Exception {
            while (isRunning) {
                int randomNum=(int)(1+Math.random()*(5-1+1));
                double a = Math.random()*(3-1)+1;
                String log = "172.31.62.243 - - [27/May/2019:14:32:21 +0000] \"POST / HTTP/1.1\" 200 200 \"page_file:\"  12104 \"recommendType=2010102&params={\\x22lang\\x22:\\x22en\\x22,\\x22pipelinecode\\x22:\\x22ZFAU\\x22,\\x22uctype\\x22:\\x22N\\x22,\\x22regioncode\\x22:\\x22AU\\x22,\\x22platform\\x22:\\x22IOS\\x22,\\x22cookie\\x22:\\x221558368574502-7978181\\x22,\\x22pageindex\\x22:0,\\x22pagesize\\x22:18,\\x22ip\\x22:\\x2260.228.50.35\\x22,\\x22ortherparams\\x22:{\\x22gender\\x22:\\x22\\x22,\\x22birthday\\x22:\\x22\\x22,\\x22adgroup\\x22:\\x22\\x22},\\x22policy\\x22:0}\" \"-\" \"-\" "+a+" 2.014 glbgcl.logsss.com";
                sourceContext.collect(LogEvent.of(nameArray[randomNum-1],System.currentTimeMillis(),log));
                num++;
                Thread.sleep(500);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }

    }


    /**
     * 参数校验
     * @param args
     * @return
     */
    public static ParameterTool parameterCheck(String[] args){

        //--bootstrap.servers 127.0.0.1:9092 --group.id test --input-event-topic timeoutAnalysisInPut --input-config-topic timeoutAnalysisConf --output-topic timeoutAnalysisOutPut

        ParameterTool params= ParameterTool.fromArgs(args);

        params.getProperties().list(System.out);

        if(!params.has(BOOTSTRAP_SERVERS)){
            System.err.println("----------------参数 [bootstrap.servers] 是必填项----------------");
            System.exit(-1);
        }
        if(!params.has(GROUP_ID)){
            System.err.println("----------------参数 [group.id] 是必填项----------------");
            System.exit(-1);
        }
        if(!params.has(INPUT_EVENT_TOPIC)){
            System.err.println("----------------参数 [input-event-topic] 是必填项----------------");
            System.exit(-1);
        }
        if(!params.has(INPUT_CONFIG_TOPIC)){
            System.err.println("----------------参数 [input-config-topic] 是必填项----------------");
            System.exit(-1);
        }
        if(!params.has(OUTPUT_TOPIC)){
            System.err.println("----------------参数 [output-topic] 是必填项----------------");
            System.exit(-1);
        }

        return params;
    }
}
