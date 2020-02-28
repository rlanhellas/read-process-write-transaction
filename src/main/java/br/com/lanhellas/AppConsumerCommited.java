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

public class AppConsumerCommited {

    private static Logger logger = LoggerFactory.getLogger(AppConsumerCommited.class);

    private static final String TOPICO_B = "topico-b";
    private static final String CONSUMER_GROUP_ID = "consumer-commited-01";
    private static final String BROKER_URL = "pkc-43n10.us-central1.gcp.confluent.cloud:9092";
    private static final String APIKEY = "H2WMLNYGYTBZGGZ5";
    private static final String SECRET = "lCbrgRJkimPI744GMXfvR4qD+M1h/EzRFfsVKFFoUXO4C/4kiWCFRlokm58OILas";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        consume();
    }

    private static void consume() throws ExecutionException, InterruptedException {
        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPICO_B));

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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "CONSUMER_COMMITED_001");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", APIKEY, SECRET));
        return new KafkaConsumer<>(props);
    }
}
