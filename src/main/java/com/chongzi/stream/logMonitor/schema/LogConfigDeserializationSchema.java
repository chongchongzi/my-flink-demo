package com.chongzi.stream.logMonitor.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.chongzi.stream.logMonitor.model.LogConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

public class LogConfigDeserializationSchema implements KeyedDeserializationSchema<LogConfig> {
    @Override
    public LogConfig deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<LogConfig>() {});
    }

    @Override
    public boolean isEndOfStream(LogConfig nextElement) {
        return false;
    }

    @Override
    public TypeInformation<LogConfig> getProducedType() {
        return TypeInformation.of(new TypeHint<LogConfig>() {});
    }
}
