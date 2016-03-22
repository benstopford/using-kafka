package com.benstopford.kafka.examples;

import static java.util.Arrays.*;
import static org.junit.Assert.*;
import static org.hamcrest.core.Is.is;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class KafkaMostBasicTest {

    public static final String topic = "topic1-" + System.currentTimeMillis();

    private KafkaTestFixture server;
    private Producer producer;
    private ConsumerConnector consumerConnector;
    KafkaConsumer<String, String> consumer;

    @Before
    public void setup() throws Exception {
        server = new KafkaTestFixture();
        server.start(serverProperties());
    }

    @After
    public void teardown() throws Exception {
        producer.close();
        if (consumerConnector != null)
            consumerConnector.shutdown();
        if (consumer != null)
            consumer.close();
        server.stop();
    }

    @Test
    public void shouldWriteThenReadOldConsumer() throws Exception {

        //Create a consumer
        ConsumerIterator<String, String> it = buildOldConsumer(topic);

        //Create a producer
        producer = new KafkaProducer(producerProps());

        //send a message
        producer.send(new ProducerRecord(topic, "message")).get();

        //read it back
        MessageAndMetadata<String, String> messageAndMetadata = it.next();
        String value = messageAndMetadata.message();
        assertThat(value, is("message"));
    }

    @Test
    public void shouldWriteThenReadNewConsumer() throws Exception {

        //Create a consumer
        consumer = new KafkaConsumer(newConsumerProperties());
        consumer.subscribe(asList(topic));
        consumer.poll(1);//initialise consumption to earliest message

        //Create a producer
        producer = new KafkaProducer(producerProps());

        //send a message
        producer.send(new ProducerRecord(topic, "message")).get();

        //read it back
        ConsumerRecord<String, String> msg = consumer.poll(1000).iterator().next();
        assertThat(msg.value(), is("message"));
    }


    private ConsumerIterator<String, String> buildOldConsumer(String topic) {
        Properties props = oldConsumerProperties();

        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, 1);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, List<KafkaStream<String, String>>> consumers = consumerConnector.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));
        KafkaStream<String, String> stream = consumers.get(topic).get(0);
        return stream.iterator();
    }

    private Properties oldConsumerProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", serverProperties().get("zookeeper.connect"));
        props.put("group.id", "group1");
        props.put("auto.offset.reset", "smallest");
        return props;
    }

    private Properties newConsumerProperties() {
        Properties props = new Properties();
        props.put("group.id", "group1");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");
        return props;
    }

    private Properties serverProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("broker.id", "1");
        return props;
    }

    private static class KafkaTestFixture {
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

}
    

