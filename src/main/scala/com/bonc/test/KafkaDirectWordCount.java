package com.bonc.test;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("kafka-wordCount1").setMaster("yarn");
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
      //  kafkaParams.put("metadata.broker.list", "ha2:9092, ha3:9092, ha4:9092");
        kafkaParams.put("bootstrap.servers", "172.16.22.171:9092,172.16.22.173:9092,172.16.22.178:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkstreaming-kafka10");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
      final Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "ha2:9092,ha3:9092,ha4:9092");
        props.put("zk.connect", "ha2:2181,ha3:2181,ha4:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");
        //可以读取多个topic
        final Set<String> topics = new HashSet<String>();
        topics.add("topic3");
        final StringBuffer strb=new StringBuffer();
    //    JavaInputDStream lines=KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(),ConsumerStrategies.Subscribe(topics,kafkaParams));
     //   Map<TopicAndPartition, Long> consumerOffsetsLong = getConsumerOffsets(kafkaCluster, serverProps, topic, groupId);
        final JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
        // 6.spark rdd转化和行动处理
      //  lines.checkpoint("5");
        lines.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD, Time time) throws Exception {
                List<ConsumerRecord<String, String>> consumerRecords = consumerRecordJavaRDD.collect();

                if(consumerRecords.size()>0) {
                    for(ConsumerRecord<String, String> consummerRecord :consumerRecords){
                       // System.out.println(consummerRecord);
                      String[] ssplit = consummerRecord.value().split(" ");//这里一定是value，不然会有consumer的系统信息
                        /*
                        * ConsumerRecord(topic = topic1, partition = 2, offset = 3023, CreateTime = 1524654452618, checksum = 675664428, serialized key size = 5, serialized value size = 99,
                         * key = 15269, value = INFO JobScheduler: Finished job streaming job 1568888888 from job set of time 1493090727000 ms15269)
                         * */
                        String info=ssplit[0]+"\t"+ssplit[1]+"\t"+ssplit[2]+"\t"+ssplit[3]+"\t"+ssplit[13];
                      //  strb.append(info+"\n");
                        Producer producer;
                        producer=new Producer<String,String>(new ProducerConfig(props));
                        producer.send(new KeyedMessage<String,String>("topic2",consummerRecord.key(),info));
                // System.out.println(info);
                    }
                }
             //   System.out.println("处理数据："+consumerRecords.size());
            //    ToHDFS(strb);//这里是用hdfs接口的时候写的，没成功
              //  strb.delete(0,strb.length());
            }
        });
       //  lines.dstream().saveAsTextFiles("hdfs://ha2:9000/spark/outfile/","spark");//把所有数据存入hdfs
      //  lines.checkpoint(new Duration(10));
      /*  lines.filter(new Function<ConsumerRecord<String, String>, Boolean>() {
            @Override
            public Boolean call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                 String message= stringStringConsumerRecord.toString();
                 message.split(" ");
                return true;
            }
        });*/
    JavaPairDStream jpds=  lines.flatMapToPair(new PairFlatMapFunction<ConsumerRecord<String,String>, String, String>() {
          @Override        //以下如果是object会有黄色背景，意味可以优化？
          public Iterator<Tuple2<String, String>> call(final ConsumerRecord<String, String> kv) throws Exception {
              List<Tuple2<String, String>> list = new ArrayList();

              if (kv==null){
                  return list.iterator();
              }
              String[] trueinfo=kv.value().split(" ");
              long plusnum=Long.valueOf(trueinfo[6])+Long.valueOf(trueinfo[12]);
             Tuple2<String,String> t=new Tuple2<String, String>(kv.key(),"相加的值是+"+plusnum);
              list.add(t);
               return list.iterator();

          }
      });

    jpds.dstream().saveAsTextFiles("hdfs://ha2:9000/spark/outfile2/","spark");//把所有数据存入hdfs;

     /*  JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Tuple2<String, String> t)
                    throws Exception {
                return  Arrays.asList(t._2.split(" ")).iterator();
            }
        });*/
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();
    }

    private static void ToHDFS(StringBuffer stringBuffer) throws IOException, URISyntaxException {
        System.out.println("调用hdfs方法成功");

        Configuration conf =new Configuration();//读取配置文件

        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");//支持追加
        conf.setBoolean("dfs.support.append", true);
        URI uri =new URI("hdfs://ha2:9000");//制定文件系统地址
        FileSystem fs1=FileSystem.get(uri,conf);
        FSDataOutputStream out=fs1.create(new Path("/spark/kafkainfo/info2.txt"));
      // System.out.println(stringBuffer.toString()+"以上是stringbuffer内部数据");


       //InputStream is=new InputBuffer();
      //  OutputStream os=new OutputBuffer();
      //  System.out.println(stringBuffer.toString().getBytes("\t"));
        System.out.println("调用hdfs方法完毕");
      // IOUtils.copyBytes(is,out,4096,true);
        //out.writeBytes(stringBuffer.toString());
      //  out.close();
       // fs1.close();
    }
}