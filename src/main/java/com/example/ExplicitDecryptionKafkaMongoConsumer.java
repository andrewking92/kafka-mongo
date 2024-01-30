package com.example;    

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.bson.Document;
import org.bson.BsonString;
import org.bson.BsonBinary;
import org.bson.types.Binary;

import com.mongodb.ConnectionString;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;

import java.io.FileInputStream;
import java.util.Collections;
import java.time.Duration;
import java.nio.charset.StandardCharsets;


public class ExplicitDecryptionKafkaMongoConsumer {
    public static void main(String[] args) throws Exception {
        Map<String, String> credentials = YourCredentials.getCredentials();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-123");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        String topicName = "encryption.inbound";

        // start-key-vault
        String keyVaultNamespace = "encryption.__keyVault";
        // end-key-vault

        String connectionString = credentials.get("MONGODB_URI");

        // start-kmsproviders
        String path = "master-key.txt";

        byte[] localMasterKeyRead = new byte[96];

        try (FileInputStream fis = new FileInputStream(path)) {
            if (fis.read(localMasterKeyRead) < 96)
                throw new Exception("Expected to read 96 bytes from file");
        }
        Map<String, Object> keyMap = new HashMap<String, Object>();
        keyMap.put("key", localMasterKeyRead);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
        kmsProviders.put("local", keyMap);
        // end-kmsproviders

        // start-extra-options
        Map<String, Object> extraOptions = new HashMap<String, Object>();
        extraOptions.put("mongocryptdSpawnPath", credentials.get("MONGOCRYPTD_PATH"));
        // end-extra-options


        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        List<String> fields = Arrays.asList("fullDocument", "encryptedField");

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(connectionString))
                        .build())
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();
        ClientEncryption mongoClientSecure = ClientEncryptions.create(clientEncryptionSettings);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, byte[]> record : records) {
                byte[] valueBytes = record.value();
                String valueString = new String(valueBytes, StandardCharsets.UTF_8);
                Document document = Document.parse(valueString);
                // System.out.println(document.toJson());

                Binary encryptedField = document.getEmbedded(fields, Binary.class);

                BsonString decryptedField = mongoClientSecure.decrypt(new BsonBinary(encryptedField.getType(), encryptedField.getData())).asString();
                // System.out.println(decryptedField.getValue());

                Document fullDocument = document.get("fullDocument", Document.class);

                String city = fullDocument.getString("city");
                String name = fullDocument.getString("name");
                Long age = fullDocument.getLong("age");

                // Create the BSON document to print out to screen
                Document finalDocument = new Document()
                    .append("name", name)
                    .append("city", city)
                    .append("age", age)
                    .append("encryptedField", decryptedField.getValue());

                System.out.println(finalDocument.toJson());

            }
        }

    }
}
