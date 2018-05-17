package com.bonc.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer1 {
    private final Producer<String, String> producer;
    public final static String TOPIC = "topic3";

    private KafkaProducer1() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "ha2:9092,ha3:9092,ha4:9092");
        props.put("zk.connect", "ha2:2181,ha3:2181,ha4:2181");

        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    private void produce() throws InterruptedException {
        int messageNo = 1000;
        final int COUNT = 1500;

        while (messageNo <= COUNT) {
            String key = String.valueOf(messageNo);
            String data = "INFO JobScheduler: Finished job streaming job 1568888888 from job set of time 1493090727000 ms" + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
            System.out.println(data);
            messageNo++;
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new KafkaProducer1().produce();
    }
}