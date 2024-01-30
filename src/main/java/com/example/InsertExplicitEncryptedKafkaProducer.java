package com.example;

/*
 * Copyright 2008-present MongoDB, Inc.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


import java.util.HashMap;
import java.util.Map;

import org.bson.BsonString;
import org.bson.BsonBinary;
import org.bson.Document;

import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.client.model.vault.EncryptOptions;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.client.model.vault.DataKeyOptions;

import java.io.FileInputStream;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.io.UnsupportedEncodingException;

public class InsertExplicitEncryptedKafkaProducer {

    public static void main(String[] args) throws Exception {
        Map<String, String> credentials = YourCredentials.getCredentials();

        String topicName = "encryption.outbound";
        String bootstrapServers = "localhost:9092";

        // Configure the Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Create the Kafka producer
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

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

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(connectionString))
                        .build())
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();
        ClientEncryption mongoClientSecure = ClientEncryptions.create(clientEncryptionSettings);


        BsonBinary dataKeyId = mongoClientSecure.createDataKey("local", new DataKeyOptions());
 
        // Explicitly encrypt a field
        BsonBinary encryptedFieldValue = mongoClientSecure.encrypt(new BsonString("123456789"),
        new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId));

        // Create the BSON document to send
        Document document = new Document("name", "John Doe")
                .append("age", 30)
                .append("city", "New York")
                .append("encryptedField", encryptedFieldValue);

        System.out.println(document.toJson());

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

        mongoClientSecure.close();
    }
}
