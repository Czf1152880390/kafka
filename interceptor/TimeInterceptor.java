package com.tmsb.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Map;


/**
 * @author Caizf
 * @date 2020/7/23 -14:23
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord<String,String> record) {
        //取出数据
        String value = record.value();
        // 创建一个新的ProducerRecord对象

        return new ProducerRecord<String, String>(record.topic(),record.partition(),
                record.key(),System.currentTimeMillis() + "," +value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
