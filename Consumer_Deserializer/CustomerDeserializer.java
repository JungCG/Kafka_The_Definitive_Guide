package com.tacademy;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Customer deserialize(String topic, byte[] data) {
        int id;
        int nameSize;
        String name;

        try {
            if (data == null)
                return null;

            if (data.length < 8) {
                throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            id = buffer.getInt();
            nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            name = new String(nameBytes, "UTF-8");

            return new Customer(id, name);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing " + "Customer to byte[] " + e);
        }
    }

    @Override
    public void close() {}
}
