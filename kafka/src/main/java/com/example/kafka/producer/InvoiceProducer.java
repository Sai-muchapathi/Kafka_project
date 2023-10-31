package com.example.kafka.producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class InvoiceProducer{
    public static void main(String[] args) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(producerProps);

        String topic = "posData";

        // Generating random POS invoice data
        Random random = new Random();
        String[] products = {"Product A", "Product B", "Product C", "Product D"};
        String[] customers = {"Customer 1", "Customer 2", "Customer 3", "Customer 4"};

        while (true) {
            String product = products[random.nextInt(products.length)];
            String customer = customers[random.nextInt(customers.length)];
            double amount = 10 + random.nextDouble() * 100;

            String posInvoiceData = "Customer: " + customer + ", Product: " + product + ", Amount: $" + amount;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, posInvoiceData);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("POS invoice data sent successfully to partition " + metadata.partition());
                    } else {
                        System.err.println("Error sending POS invoice data: " + exception.getMessage());
                    }
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
