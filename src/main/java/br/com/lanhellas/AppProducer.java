package br.com.lanhellas;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AppProducer {

    private static Logger logger = LoggerFactory.getLogger(AppProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        produce();
    }

    private static void produce() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> kafkaProducer = createProducer();
        for(int i = 0; i < 900000; i++) {
            kafkaProducer.send(new ProducerRecord<>(Configuration.INPUT_TOPIC, "A"));
        }

        kafkaProducer.flush();
    }

    private static KafkaProducer<String, String> createProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BROKER_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "PRODUCER_INPUT_TOPIC");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", Configuration.APIKEY, Configuration.SECRET));
        return new KafkaProducer<>(props);
    }
}
