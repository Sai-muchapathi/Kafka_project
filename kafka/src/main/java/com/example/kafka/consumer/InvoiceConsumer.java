package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.Properties;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;


public class InvoiceConsumer {
    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pos-consumer-group"); // Consumer group ID
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        String topic = "posData";
        consumer.subscribe(Collections.singletonList(topic));

        CqlSession session = CqlSession.builder().build();

        String cassandraKeyspace = "SimpleStrategy";
        String cassandraTable = "pos_data";
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received POS invoice data: " + record.value());
                }
                for (ConsumerRecord<String, String> record : records) {
                    String kafkaMessage = record.value();
                    SimpleStatement statement = SimpleStatement.builder("INSERT INTO " + cassandraKeyspace + "." + cassandraTable + " (message) VALUES (?)")
                            .addPositionalValue(kafkaMessage)
                            .build();
                    session.execute(statement);
                    System.out.println("Received and stored Kafka message in Cassandra posData table: " + kafkaMessage);
                }
            }
        } finally {
            consumer.close();
            session.close();
        }
    }
}
