package br.com.lanhellas;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AppReadProcessWrite {

    private static final String TOPICO_A = "topico-a";
    private static final String TOPICO_B = "topico-b";
    private static final String TRANSACTION_ID = "read-process-write-tran-01";
    private static final String CONSUMER_GROUP_ID = "consumer-rpw-01";
    private static final String BROKER_URL = "pkc-43n10.us-central1.gcp.confluent.cloud:9092";
    private static final String APIKEY = "H2WMLNYGYTBZGGZ5";
    private static final String SECRET = "lCbrgRJkimPI744GMXfvR4qD+M1h/EzRFfsVKFFoUXO4C/4kiWCFRlokm58OILas";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        process();
    }

    private static void process() throws ExecutionException, InterruptedException {
        //Cria o produtor
        KafkaProducer<String, String> kafkaProducer = createProducer();

        //Inicializa as configurações deste produtor no Coordinator do Cluster
        kafkaProducer.initTransactions();

        //Cria o Consumidor e inscreve o mesmo no TOPICO_A
        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPICO_A));

        while (true) {
            //Map responsável por guardar quais offsets devem ser comitados no fim da transação ('All or Nothing')
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

            //Aguarda por no máximo 1minuto por mensagens a serem consumidas
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Long.MAX_VALUE);

            //Ao chegar neste ponto significa que mensagens estão sendo consumidas, então podemos iniciar uma transação
            kafkaProducer.beginTransaction();
            boolean aborted = false;
            for (ConsumerRecord<String, String> record : records) {

                //Para cada mensagem consumida nós realizamos uma 'transformação simples', sem comitar o offset de consumo
                String message = transformMessage(record.value());
                kafkaProducer.send(new ProducerRecord<>(TOPICO_B, record.key(), message));

                //todas as mensagens consumidas e produzidas dentro dessa transação são abortadas se conter um valor que desejamos testar
                if (message.contains("55")) {
                    kafkaProducer.abortTransaction();
                    aborted = true;
                    break;
                }


                //Sempre que uma mensagem for 'transformada' com sucesso, gravamos o offset dela no nosso Map, para futuro commit da transação
                offsets.put(new TopicPartition(TOPICO_A, record.partition()), new OffsetAndMetadata(record.offset()));
            }

            //Após tudo finalizado com sucesso, podemos comitar a transação, mas precisamos dizer ao Coordinator quais os offsets devem
            //ser comitados.
            if (!aborted) {
                kafkaProducer.sendOffsetsToTransaction(offsets, CONSUMER_GROUP_ID);
                kafkaProducer.commitTransaction();
            }
        }

    }

    private static String transformMessage(String message) {
        return message.concat("-processed");
    }

    private static KafkaProducer<String, String> createProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "PRODUCER_001");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID);
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", APIKEY, SECRET));
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "CONSUMER_001");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", APIKEY, SECRET));
        return new KafkaConsumer<>(props);
    }
}
