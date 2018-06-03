package com.sundar.log.consumer;

import com.sundar.log.bean.LogProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

/**
 *
 * @author sundar
 * @since 2018-01-28
 */
public class ReadFromKafkaByConsumer {

    private final static Logger LOG = Logger.getLogger(ReadFromKafkaByConsumer.class);
    private static String TOPIC = null;
    private static String BOOTSTRAP_SERVERS = null;
    private static String MAX_RECORD = null;
    private static String GROUP_ID_CONFIG = null;
    private static long SLEPPING_TIME = 0;

    public ReadFromKafkaByConsumer() {
        try {
            Properties pro = new Properties();
            pro.load(ReadFromKafkaByConsumer.class.getResourceAsStream("/conf/consumerconf.properties"));
            ReadFromKafkaByConsumer.BOOTSTRAP_SERVERS = pro.getProperty("BOOTSTRAP_SERVERS");
            ReadFromKafkaByConsumer.TOPIC = pro.getProperty("TOPIC");
            ReadFromKafkaByConsumer.MAX_RECORD = pro.getProperty("MAX_RECORD");
            ReadFromKafkaByConsumer.SLEPPING_TIME = Long.parseLong(pro.getProperty("SLEPPING_TIME"));
            ReadFromKafkaByConsumer.GROUP_ID_CONFIG = pro.getProperty("GROUP_ID_CONFIG");
        } catch (IOException ex) {
            LOG.error("Exception occurred while load Properties : " + ex, ex);
        }
    }

    private static Consumer<Long, List<LogProperties>> createConsumer() {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ReadFromKafkaByConsumer.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_RECORD);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, List<LogProperties>> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        List<String> list = new ArrayList<>();
        list.add(ReadFromKafkaByConsumer.TOPIC);
//        list.add(ReadFromKafkaByConsumer.TOPIC1);
        consumer.subscribe(list);
        //subscribe more than one topics
//        consumer.subscribe(Arrays.asList(ReadFromKafkaByConsumer.TOPIC, ReadFromKafkaByConsumer.TOPIC1));

        return consumer;
    }

    private void readRecord() {
        try (Consumer<Long, List<LogProperties>> consumer = createConsumer()) {
            final int giveUp = 100;
            int noRecordsCount = 0;
            while (true) {
                final ConsumerRecords<Long, List<LogProperties>> consumerRecords = consumer.poll(ReadFromKafkaByConsumer.SLEPPING_TIME);
                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) {
                        break;
                    }
                    consumerRecords.forEach((record) -> {
                        List<LogProperties> kafkaRecords = record.value();
                        LOG.info("Consumer Record:(" + record.key() + ", " + record.value() + ", " + record.partition()
                                + ", " + record.offset() + ")");
                        kafkaRecords.forEach(action -> {
                            if ("INFO".equals(action.getSeverity())) {

                            } else {

                            }
                            LOG.info("Log Severity : [" + action.getSeverity() + "] TimeStamp : [" + action.getOccurranceDate()
                                    + "] className : [" + action.getClassName() + "] Message : [" + action.getMessage() + "]");
                        });
                    });
                    consumer.commitAsync();
                }

            }
        }
        LOG.info("DONE");
    }

    public static void main(String[] args) {
        try {
            new ReadFromKafkaByConsumer().readRecord();
        } catch (Exception ex) {
            LOG.info("Exception occurred while read record from KAFKA : " + ex, ex);
        }
    }
}
