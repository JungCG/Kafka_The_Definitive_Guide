package com.tacademy;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    // TOPIC_NAME
    private static String TOPIC_NAME = "test";
    // CONSUMER_GROUP_NAME
    private static String GROUP_ID = "cgtest";
    // BOOTSTRAP_SERVER
    private static String BOOTSTRAP_SERVERS = "{AWS EC2 Public IP}:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // KEY_DESERAILIZER
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // VALUE_DESERIALIZER
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        try {

            while (true) {
                // Duration.ofSeconds(1) : Interval = 1 sec
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic : " + record.topic());
                    System.out.println("partition : " + record.partition());
                    System.out.println("offset : " + record.offset());
                    System.out.println("key : " + record.key());
                    System.out.println("record : " + record.value());
                    System.out.println("===================");
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }finally{
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }
}