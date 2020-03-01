package br.com.lanhellas;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AppReadProcessWriteNoTransaction {

    private static final String CONSUMER_GROUP_ID = "consumer-group-notransact-03";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long init = System.currentTimeMillis();
        process();
        long end = System.currentTimeMillis();
        System.out.println("Total time (ms) = "+(end - init));
    }

    private static void process() throws ExecutionException, InterruptedException {
        //Cria o produtor
        KafkaProducer<String, String> kafkaProducer = createProducer();

        //Cria o Consumidor e inscreve o mesmo no INPUT-TOPIC
        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singleton(Configuration.INPUT_TOPIC));

        int recordsCount = 0;
        do{

            //Aguarda por no máximo 1minuto por mensagens a serem consumidas
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(10));
            recordsCount = records.count();

            for (ConsumerRecord<String, String> record : records) {

                //Para cada mensagem consumida nós realizamos uma 'transformação simples', sem comitar o offset de consumo
                String message = transformMessage(record.value());
                kafkaProducer.send(new ProducerRecord<>(Configuration.OUTPUT_TOPIC, message));

                //todas as mensagens consumidas e produzidas dentro dessa transação são abortadas se conter um valor que desejamos testar
                if (message.contains("55")) {
                    break;
                }
            }
        }while (recordsCount > 0);

        kafkaProducer.flush();

    }

    private static String transformMessage(String message) {
        return message.concat("-processed");
    }

    private static KafkaProducer<String, String> createProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BROKER_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "PRODUCER_NOTRAN_OUTPUT_TOPIC");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", Configuration.APIKEY, Configuration.SECRET));
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BROKER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "CONSUMER_NOTRAN_INPUT_TOPIC-02");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", Configuration.APIKEY, Configuration.SECRET));
        return new KafkaConsumer<>(props);
    }
}
