package com.chongzi.stream.logMonitor.function;

import com.chongzi.stream.logMonitor.LogMonitor;
import com.chongzi.stream.logMonitor.model.LogConfig;
import com.chongzi.stream.logMonitor.model.LogEvaluatedResult;
import com.chongzi.stream.logMonitor.model.LogEvent;
import com.chongzi.stream.logMonitor.model.LogEventContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @Description 连接流处理逻辑
 * @Author chongzi
 * @Date 2019/5/28 20:55
 * @Param 
 * @return 
 **/
@Slf4j
public class LogConnectedBroadcastProcessFuntion extends KeyedBroadcastProcessFunction<String, LogEvent, LogConfig, LogEvaluatedResult> {

    private LogConfig defaultLogConfig = new LogConfig("张三","测试","\"-\" \"-\" 2.","推荐接口在10分钟内超时大于等于30次",2,"2019-05-29",10,5,"","613032","");


    private final MapStateDescriptor<String, Map<String, LogEventContainer>> logEventMapStateDesc =
            new MapStateDescriptor<>(
                    "logEventContainerState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<>(String.class, LogEventContainer.class));

    /**
     * 处理事件流
     * @param value 事件
     * @param ctx 上下文
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(LogEvent value, ReadOnlyContext ctx, Collector<LogEvaluatedResult> out) throws Exception {
        String applicationName = value.getApplicationName();
        String logStr = value.getLog();
        LogConfig config = ctx.getBroadcastState(LogMonitor.logConfigStateDescriptor).get(applicationName);
        if (Objects.isNull(config)) {
            config = defaultLogConfig;
        }
        String alarmKeywordRule = config.getAlarmKeywordRule();
        int timeLength = config.getTimeLength() * 1000;
        int num = config.getNum();
        int alarmType = config.getAlarmType();

        final MapState<String, Map<String, LogEventContainer>> LogEventMapState =
                getRuntimeContext().getMapState(logEventMapStateDesc);
        if (alarmType == 2 && logStr.contains(alarmKeywordRule)) {
            Map<String, LogEventContainer> logEventContainerMap = LogEventMapState.get(applicationName);
            if (Objects.isNull(logEventContainerMap)) {
                logEventContainerMap = Maps.newHashMap();
                LogEventMapState.put(applicationName, logEventContainerMap);
            }
            if (!logEventContainerMap.containsKey(applicationName)) {
                LogEventContainer container = new LogEventContainer();
                container.setKey(applicationName);
                container.setStartTime(ctx.timestamp());
                logEventContainerMap.put(applicationName, container);
            }
            LogEventContainer logEventContainer = logEventContainerMap.get(applicationName);
            logEventContainer.getLogEvents().add(value);
            logEventContainer.count ++;
            logEventContainer.lastModified = ctx.timestamp();
            int  maxLen = logEventContainerMap.get(applicationName).getLogEvents().size();
            long runTimeLength = logEventContainer.lastModified - logEventContainer.startTime;
            log.info(applicationName + "，运行时长: "+runTimeLength+ "，配置时长: "+timeLength+ "，事件次数: "+maxLen+ "，配置次数: "+num);

            // 如果运行时长大于等于配置时长
            if(timeLength >= runTimeLength) {
                // 如果采集条数大于等于配置条数
                if(maxLen >= num){
                    Optional<LogEvaluatedResult> result = compute(config, logEventContainerMap.get(applicationName));
                    log.info("检测到符合规则的事件：" + applicationName);
                    result.ifPresent(r -> out.collect(result.get()));
                    // 是否匹配都重置重新计算
                    LogEventMapState.get(applicationName).remove(applicationName);
                }
            }else {
                // 是否匹配都重置重新计算
                LogEventMapState.get(applicationName).remove(applicationName);
            }
        }
    }

    /**
     * 处理配置流
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(LogConfig value, Context ctx, Collector<LogEvaluatedResult> out) throws Exception {
        String applicationName = value.getApplicationName();
        BroadcastState<String, LogConfig> state = ctx.getBroadcastState(LogMonitor.logConfigStateDescriptor);
        final LogConfig oldLogConfig = ctx.getBroadcastState(LogMonitor.logConfigStateDescriptor).get(applicationName);
        if(state.contains(applicationName)) {
            log.info("LogConfigured serviceName exists: serviceName=" + applicationName);
            log.info("LogConfig detail: oldLogConfig=" + oldLogConfig + ", newLogConfig=" + value);
        }else {
            log.info("LogConfig detail: defaultLogConfig=" + defaultLogConfig + ", newLogConfig=" + value);
        }
        // update config value for configKey
        state.put(applicationName, value);
    }



    /**
     * 计算超时次数
     * @param config
     * @param container
     * @return
     */
    private Optional<LogEvaluatedResult> compute(LogConfig config, LogEventContainer container) {
        // 防止flink空指针，设置一个空对象
        Optional<LogEvaluatedResult> result = Optional.empty();
        int num = config.getNum();
        int maxTimeOutLen = container.getLogEvents().size();
        log.info("配置多少条："+num);
        log.info(container.key+"有多少条："+maxTimeOutLen);
        // 超出次数发送告警
        container.getLogEvents().sort(Comparator.comparingLong(LogEvent::getEventTime));
        final LogEvaluatedResult evaluatedResult = new LogEvaluatedResult();
        evaluatedResult.setApplicationName(container.getKey());
        evaluatedResult.setLogEvents(container.getLogEvents());
        //log.info("Evaluated result: " + JSON.toJSONString(evaluatedResult));
        result = Optional.of(evaluatedResult);
        return result;
    }

}
