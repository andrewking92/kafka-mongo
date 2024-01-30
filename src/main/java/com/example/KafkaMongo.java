package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaMongo {

    public static void main(String[] args) {

        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.JsonSerializer");

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topicName = "encryption.outbound";
        String message = "Hello, Kafka!";

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
        producer.send(record);

        // Close Kafka producer
        producer.close();
    }
}
