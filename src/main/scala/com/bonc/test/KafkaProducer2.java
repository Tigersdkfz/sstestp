package com.bonc.test;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducer2 {

    public final static String TOPIC = "topic3";

    private KafkaProducer2() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "ha2:9092,ha3:9092,ha4:9092");
        props.put("zk.connect", "ha2:2181,ha3:2181,ha4:2181");

        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "-1");

        private Producer<String,String> producer;
      //  producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));

    }

    private void produce() throws InterruptedException {
        int messageNo = 1501;
        final int COUNT = 2000;
        //ProducerRecord<String,String> pr;
        while (messageNo <= COUNT) {
            String key = String.valueOf(messageNo);
            String data = "INFO JobScheduler: Finished job streaming job 1568888888 from job set of time 1493090727000 ms" + key;
            ProducerRecord<String,String> pr=new ProducerRecord("topic3",key,data);
           // producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
             producer.send(pr);
            System.out.println(data);
            messageNo++;
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new KafkaProducer2().produce();
    }
}