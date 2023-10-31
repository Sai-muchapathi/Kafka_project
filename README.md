# Kafka_project

The producer generates random stream of POS Invoice data that contains **customer ID, product ID, and the amount**.
The generated data is pushed to the **Kafka topic** which is then **polled by the consumer**.
**Cassandra** is used by the consumer to store the received data.

**Please find the Cassandra configuration below**
-- Create a keyspace
**CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };**

-- Create a table
**CREATE TABLE IF NOT EXISTS store.pos_data (
customerId text PRIMARY KEY,
productId text,
amount double
);**


**Add Cassandra and Kafka dependencies inside pom.xml file**

<dependency>
			<groupId>com.datastax.cassandra</groupId>
			<artifactId>cassandra-driver-core</artifactId>
			<version>4.0.0</version>
			<type>pom</type>
</dependency>


<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
</dependency>
