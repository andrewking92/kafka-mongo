package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.bson.Document;


public class KafkaJsonConsumer {

    private final String topic;
    private final Consumer<String, String> consumer;

    public KafkaJsonConsumer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker URL
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-433"); // Replace with your own group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // always consume from the beginning of the topic
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public JSONObject receive() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            JSONObject payload = new JSONObject(record.value());
            // System.out.println("Received message: " + payload.toString());
            return payload;
        }
        return null;
    }

    public void close() {
        consumer.close();
    }

    public static void main(String[] args) {
        KafkaJsonConsumer consumer = new KafkaJsonConsumer("encryption.inbound");
        try {
            while (true) {
                JSONObject payload = consumer.receive();
                if (payload != null) {
                    // Do something with the payload
                    System.out.println("*****\n\n*****\n\n*****\n\nRECEIVED ENCRYPTED DOCUMENT: \n\n" + payload + "\n\n*****\n\n*****\n\n*****\n\n");
                    // System.out.println("Received payload: " + payload.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
