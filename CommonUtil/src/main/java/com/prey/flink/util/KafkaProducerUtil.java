package com.prey.flink.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.util.Properties;

public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("topic ");
    }

    public static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.server","localhost:9092");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 读取文本
        URL url = KafkaProducerUtil.class.getResource("data.txt");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(url.getPath()));
        String line;
        while((line = bufferedReader.readLine())!=null){
            ProducerRecord<String , String> producerRecord = new ProducerRecord<>(topic , line);
            producer.send(producerRecord);
        }
        producer.close();
    }
}
