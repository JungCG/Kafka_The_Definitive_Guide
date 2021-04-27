package com.tacademy;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroDeserializer {
    // TOPIC_NAME
    private static String TOPIC_NAME = "test";
    // BOOTSTRAP_SERVERS : Kafka Cluster IP = AWS EC2 Public IP
    private static String BOOTSTRAP_SERVERS = "{AWS EC2 Public IP}}:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        // BOOTSTRAP_SERVERS
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // KEY_SERIALIZER
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // VALUE_SERIALIZER
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // SCHEMA_REGISTRY_URL
        configs.put("schema.registry.url", "{schemaUrl}");
//        configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "{schemaUrl}");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, Customer> records = consumer.poll(1000);

            for (ConsumerRecord<String, Customer> record : records) {
                // 데이터 처리
            }

            consumer.commitSync();
        }
    }
}
