package com.tacademy;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class GenericAvroSerializer {
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

        String schemaString =
                "{\"namespace\": \"customerManagement.avro\", " +
                "\"type\": \"record\", " +
                "\"name\": \"Customer\", " +
                "\"fields\": [" +
                "{\"name\": \"id\", \"type\": \"int\"}," +
                "{\"name\": \"name\", \"type\": \"string\"," +
                "{\"name\": \"email\", \"type\": " + "[\"null\",\"string\"], " +
                "\"default\": \"null\" }" +
                "]}";

        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(configs);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        for(int i = 0 ; i<10;i++){
            GenericRecord customer = new GenericData.Record(schema);
            String name = "testName";
            customer.put("id", i);
            customer.put("name", name);
            customer.put("email", "testEmail");

            ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(TOPIC_NAME, name, customer);

            producer.send(record);
        }
    }
}
