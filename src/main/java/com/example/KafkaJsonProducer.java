package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;

public class KafkaJsonProducer {

    private final String topic;
    private final Producer<String, String> producer;

    public KafkaJsonProducer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker URL
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void send(JSONObject payload) {
        String payloadStr = payload.toString();
        producer.send(new ProducerRecord<String, String>(topic, payloadStr), (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());
            } else {
                System.err.println("Failed to send message: " + exception.getMessage());
            }
        });
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        KafkaJsonProducer producer = new KafkaJsonProducer("encryption.outbound");
        
        // Create a sample JSON payload
        JSONObject payload = new JSONObject();
        payload.put("name", "John Doe");
        payload.put("age", 30);
        payload.put("email", "johndoe@example.com");
        
        // Send the payload to Kafka
        producer.send(payload);
        
        // Close the producer when finished
        producer.close();
    }
}
