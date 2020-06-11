package com.klotz.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10 ; i++) {

            String topic = "second_topic";
            String value = "hello World" + Integer.toString(i);
            String key = "id_" + Integer.toString(i); //providing a key, we guaranty that the same key always goes
            // to the same partition

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: "  + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing:", e);
                    }
                }
            }).get(); //block the send to make it synchronous - don`t do this in production
        }


        //kafka produces messages asynchronously, thus if we want to send the message before
        //the application terminates, we need to use the flush method, this will force kafka
        //to send all messages
        producer.flush();

        //closes the producer;
        producer.close();
    }
}
