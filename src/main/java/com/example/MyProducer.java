package com.example;

import org.apache.kafka.clients.producer.*;
import org.bson.Document;
import java.util.Properties;
import java.io.UnsupportedEncodingException;


public class MyProducer {
    public static void main(String[] args) {
        String topicName = "encryption.inbound";
        String bootstrapServers = "localhost:9092";

        // Configure the Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Create the Kafka producer
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        // Create the BSON document to send
        Document document = new Document("name", "John Doe")
                .append("age", 30)
                .append("city", "New York");

        try {
            // Serialize the BSON document to a byte array
            byte[] value = document.toJson().getBytes("UTF-8");

            // Create the Kafka record
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, value);

            // Send the Kafka record
            producer.send(record);
        } catch (UnsupportedEncodingException e) {
            // TODO: handle exception
        } finally {
            // Close the Kafka producer
            producer.close();
        }
    }
}
