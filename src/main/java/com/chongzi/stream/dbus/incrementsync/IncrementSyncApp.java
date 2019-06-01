package com.chongzi.stream.dbus.incrementsync;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.chongzi.stream.dbus.canal.FlatMessageSchema;
import com.chongzi.stream.dbus.function.DbusProcessFuntion;
import com.chongzi.stream.dbus.model.Flow;
import com.chongzi.stream.dbus.sink.HbaseSyncSink;
import com.chongzi.stream.dbus.source.FlowSoure;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/3 下午1:54
 * 增量同步flink app
 */
public class IncrementSyncApp {
    public static final String TOPIC = "example";

    public static final MapStateDescriptor<String, Flow> flowStateDescriptor =
            new MapStateDescriptor<>(
                    "flowBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Flow>() {}));


    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "group18");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("flink.partition-discovery.interval-millis","30000");

        FlinkKafkaConsumer011<FlatMessage> myConsumer=new FlinkKafkaConsumer011<>(
                TOPIC,
                new FlatMessageSchema(),
                props
        );

        //FlatMessage事件流
        DataStream<FlatMessage> meaasge=env.addSource(myConsumer);

        //keyby：同一个库，同一个表的FlatMessage会
        KeyedStream<FlatMessage,String> keyedMessage=meaasge.keyBy(new KeySelector<FlatMessage, String>() {
                    @Override
                    public String getKey(FlatMessage value) throws Exception {
                        return value.getDatabase()+value.getTable();
                    }
                });

        //Flow配置流：Flow由配置管理模块维护在数据库里
        final BroadcastStream<Flow> flowBroadcastStream = env.addSource(new FlowSoure()).broadcast(flowStateDescriptor);

        //连接两个流并过滤不需要同步的数据
        DataStream<Tuple2<FlatMessage,Flow>> connectedStream = keyedMessage
                .connect(flowBroadcastStream)
                .process(new DbusProcessFuntion())
                .setParallelism(1);

//        connectedStream.print();


        connectedStream.addSink(new HbaseSyncSink());

        env.execute("Flink add sink");
    }
}
