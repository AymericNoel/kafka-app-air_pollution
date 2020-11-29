package collection.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static config.KafkaConfig.BOOTSTRAP_SERVERS;
import static config.KafkaConfig.RAW_TOPIC_NAME;
import static config.KafkaConfig.GROUP_ID;;


//to see if correct raw data are able to be consumed before to be analysed
public class RawDataConsumerUtils {

    public static void main(String[] args) {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using properties.
        final Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(RAW_TOPIC_NAME));

        final AtomicInteger counter = new AtomicInteger(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            System.out.println("Nb elements: " + counter.get());
        }));

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                counter.incrementAndGet();
                System.out.println(consumerRecord);
            }
        }
    }
}
