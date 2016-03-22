package com.benstopford.kafka.examples.util;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.Properties;

public class MiniKafka {
    private TestingServer zk;
    private KafkaServerStartable kafka;

    public void start(Properties properties) throws Exception {
        Integer port = getZkPort(properties);
        zk = new TestingServer(port);
        zk.start();

        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();
    }

    public void stop() throws IOException {
        kafka.shutdown();
        zk.stop();
        zk.close();
    }

    private int getZkPort(Properties properties) {
        String url = (String) properties.get("zookeeper.connect");
        String port = url.split(":")[1];
        return Integer.valueOf(port);
    }
}