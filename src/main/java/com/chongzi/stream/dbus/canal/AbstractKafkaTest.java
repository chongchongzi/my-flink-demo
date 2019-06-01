package com.chongzi.stream.dbus.canal;

import org.junit.Assert;

/**
 * Kafka 测试基类
 */
public abstract class AbstractKafkaTest {

    public static String  topic     = "example";
    public static Integer partition = null;
    public static String  groupId   = "g5";
    public static String  servers   = "127.0.0.1:9092";
    public static String  zkServers = "127.0.0.1:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}