package com.tmsb.kafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Caizf
 * @date 2020/7/13 -11:27
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //kafka集群,broker-list
        props.put("bootstrap.servers", "node07:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", "1");
        //批次大小
        props.put("batch.size", "16384");
        //等待时间
        props.put("linger,ms", "1");
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 30; i++) {

            producer.send(new ProducerRecord<>("TEST", "guigu","guigu---"+i));
        }
        producer.close();

    }

}