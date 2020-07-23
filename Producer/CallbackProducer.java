package com.tmsb.kafka.Producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author Caizf
 * @date 2020/7/13 -14:03
 */
public class CallbackProducer {
    public static void main(String[] args) throws Exception {
        //创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node07:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者对象
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        //发送数据
        for (int i = 0; i < 30; i++) {

            producer.send(new ProducerRecord<>("TEST",0,"atguigu","atguigy---"+i),
                    (recordMetadata, e) -> {
                if(e ==null){
                    System.out.println(recordMetadata.partition()
                            +"-----------"+recordMetadata.offset());
                }else{
                    e.printStackTrace();
                }
            });
        }
        //关闭资源
        producer.close();
    }


}

