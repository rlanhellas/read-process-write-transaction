package br.com.lanhellas;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AppConsumerCommitted {

    private static Logger logger = LoggerFactory.getLogger(AppConsumerCommitted.class);

    private static final String CONSUMER_GROUP_ID = "consumer-committed-01";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        consume();
    }

    private static void consume() throws ExecutionException, InterruptedException {
        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singleton(Configuration.OUTPUT_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Consumindo mensagem comitada = {}", record.value());
            }

            kafkaConsumer.commitSync();
        }

    }

    private static KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BROKER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "CONSUMER_COMMITTED_001");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", Configuration.APIKEY, Configuration.SECRET));
        return new KafkaConsumer<>(props);
    }
}
