package com.zlab.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeekDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);

        String bootstrapServer = "localhost:9092";
        String topic = "first_topic";

        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 2L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 2;
        boolean keepReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while(keepReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : records){
                logger.info("Key: "+record.key() +" -> "+"Value: "+record.value());
                logger.info("Partition: "+record.partition()+" | "+" Offset: "+record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepReading = false;
                    break;
                }
            }
        }

    }
}
