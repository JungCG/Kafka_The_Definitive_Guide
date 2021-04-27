package com.tacademy;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IndependentConsumer {
    // TOPIC_NAME
    private static String TOPIC_NAME = "TopicTest";

    // CONSUMER_GROUP_NAME (not used)
    // private static String GROUP_ID = "cgtest";

    // BOOTSTRAP_SERVER
    private static String BOOTSTRAP_SERVERS = "{AWS EC2 Public IP}:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // KEY_DESERAILIZER
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // VALUE_DESERIALIZER
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        List<PartitionInfo> partitionInfos = null;
        partitionInfos = consumer.partitionsFor(TOPIC_NAME);

        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
        }

        consumer.assign(partitions);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic : " + record.topic());
                    System.out.println("partition : " + record.partition());
                    System.out.println("offset : " + record.offset());
                    System.out.println("key : " + record.key());
                    System.out.println("record : " + record.value());
                    System.out.println("===================");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }
}
