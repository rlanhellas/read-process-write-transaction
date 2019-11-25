package br.com.lanhellas;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {
        process();
    }

    private static void process() throws ExecutionException, InterruptedException {
        KafkaProducer<String,String> kafkaProducer = createProducer();
        kafkaProducer.initTransactions();

        KafkaConsumer<String,String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singleton("topic-a"));

        while(true){
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMinutes(1));
            kafkaProducer.beginTransaction();
            for(ConsumerRecord<String,String> record : records){
                Future<RecordMetadata> futureRecord = kafkaProducer.send(new ProducerRecord<>("topic-b", record.key(),
                        record.value().concat("-processed")));
                offsets.put(new TopicPartition("topic-b",futureRecord.get().partition()),
                        new OffsetAndMetadata(futureRecord.get().offset()));
            }

            kafkaProducer.sendOffsetsToTransaction(offsets, "lanhellas-consumer-001");
            kafkaProducer.commitTransaction();
        }

    }

    private static KafkaProducer<String,String> createProducer(){
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client001");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lanhellas-transaction-001");
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String,String> createConsumer(){
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lanhellas-consumer-001");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
