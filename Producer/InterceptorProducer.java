package com.tmsb.kafka.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author Caizf
 * @date 2020/7/23 -14:48
 */
public class InterceptorProducer {
    public static void main(String[] args) {
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
        //Key,Value的序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.tmsb.kafka.interceptor.CounterInterceptor");
        interceptors.add("com.tmsb.kafka.interceptor.TimeInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 30; i++) {

            producer.send(new ProducerRecord<>("TEST", "guigu","guigu---"+i));
        }
        //关闭资源
        producer.close();
    }
}
