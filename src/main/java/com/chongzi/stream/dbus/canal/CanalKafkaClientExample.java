package com.chongzi.stream.dbus.canal;

import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.util.Assert;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;

@Slf4j
public class CanalKafkaClientExample {

    private KafkaCanalConnector             connector;

    private static volatile boolean         running = false;

    private Thread                          thread  = null;

    private Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

                                                        @Override
                                                        public void uncaughtException(Thread t, Throwable e) {
                                                            log.error("parse events has an error", e);
                                                        }
                                                    };

    public CanalKafkaClientExample(String zkServers, String servers, String topic, Integer partition, String groupId){
        connector = new KafkaCanalConnector(servers, topic, partition, groupId, null, true);
    }

    public static void main(String[] args) {
        try {
            final CanalKafkaClientExample kafkaCanalClientExample = new CanalKafkaClientExample(
                AbstractKafkaTest.zkServers,
                AbstractKafkaTest.servers,
                AbstractKafkaTest.topic,
                AbstractKafkaTest.partition,
                AbstractKafkaTest.groupId);
            log.info("## start the kafka consumer: {}-{}", AbstractKafkaTest.topic, AbstractKafkaTest.groupId);
            kafkaCanalClientExample.start();
            log.info("## the canal kafka consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        log.info("## stop the kafka consumer");
                        kafkaCanalClientExample.stop();
                    } catch (Throwable e) {
                        log.warn("##something goes wrong when stopping kafka consumer:", e);
                    } finally {
                        log.info("## kafka consumer is down.");
                    }
                }

            });
            while (running)
                ;
        } catch (Throwable e) {
            log.error("## Something goes wrong when starting up the kafka consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<FlatMessage> messages = connector.getFlatList(100L, TimeUnit.MILLISECONDS); // 获取message
                        if (messages == null) {
                            continue;
                        }
                        for (FlatMessage message : messages) {
//                            long batchId = message.getId();
                            log.info("-----------"+message.toString());
                        }

                        connector.ack(); // 提交确认
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        try {
            connector.unsubscribe();
        } catch (WakeupException e) {
            // No-op. Continue process
        }
        connector.disconnect();
    }
}