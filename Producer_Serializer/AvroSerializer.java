package com.tacademy;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroSerializer {
    // TOPIC_NAME
    private static String TOPIC_NAME = "test";
    // BOOTSTRAP_SERVERS : Kafka Cluster IP = AWS EC2 Public IP
    private static String BOOTSTRAP_SERVERS = "{AWS EC2 Public IP}}:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        // BOOTSTRAP_SERVERS
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // KEY_SERIALIZER
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // VALUE_SERIALIZER
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // SCHEMA_REGISTRY_URL
        configs.put("schema.registry.url", "{schemaUrl}");
//        configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "{schemaUrl}");


        KafkaProducer<String, Customer> producer = new KafkaProducer<String, Customer>(configs);

        for(int i = 0 ; i<10;i++){
            Customer customer = new Customer(i, "idx"+i);
            System.out.println(customer.toString());

            ProducerRecord<String, Customer> record = new ProducerRecord<>(TOPIC_NAME, customer.getName(), customer);
            producer.send(record);
        }
    }
}
