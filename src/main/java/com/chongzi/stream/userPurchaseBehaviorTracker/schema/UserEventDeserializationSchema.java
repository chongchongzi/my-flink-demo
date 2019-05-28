package com.chongzi.stream.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.chongzi.stream.userPurchaseBehaviorTracker.model.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

public class UserEventDeserializationSchema implements KeyedDeserializationSchema<UserEvent> {
    /**
     * 反序列化
     * @param messageKey
     * @param message
     * @param topic
     * @param partition
     * @param offset
     * @return
     * @throws IOException
     */
    @Override
    public UserEvent deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<UserEvent>() {});
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {});
    }
}
