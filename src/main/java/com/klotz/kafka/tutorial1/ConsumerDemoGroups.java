package com.klotz.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoGroups {
    public static void main(String[] args) {

        new ConsumerDemoGroups().run();
    }

    private ConsumerDemoGroups() { }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "second_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerRunnable(topic, bootstrapServer, groupId, latch);

        //starts the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application was interrupted", e);
        } finally {
            logger.info("Application is closing...");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(
                String topic,
                String bootstrapServer,
                String groupId,
                CountDownLatch latch) {
            this.latch = latch;

            //create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create a consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            //subscribe to our topics
            consumer.subscribe(Arrays.asList(topic)); //here we can include as many topics as we want

        }

        @Override
        public void run() {
            //poll for new data
           try {
               while (true) {
                   ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                   for(ConsumerRecord<String, String> record : records) {
                       logger.info(" Key: " + record.key() +
                               " Value: " + record.value() +
                               " Partition: " + record.partition() +
                               " Offsets: " + record.offset());
                   }
               }
           }
           catch (WakeupException e) {
               logger.info("Received shutdown signal!");
           }
           finally {
               consumer.close();
               //tell our code that we`re done with the consumer
               latch.countDown();
           }
        }

        public void shutdown() {
            //the wakeup call interrupt the consumer.poll
            //It will throw an exception WakeupException
            consumer.wakeup();
        }
    }
}
