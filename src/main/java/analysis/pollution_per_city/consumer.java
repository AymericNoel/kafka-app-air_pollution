package analysis.pollution_per_city;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static analysis.pollution_per_city.producer.STREAM_APP_1_OUT;
import static config.KafkaConfig.BOOTSTRAP_SERVERS;
import static config.KafkaConfig.GROUP_ID;


class consumer {

    public static void main(String[] args) {

        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());

        final Consumer<String, Double> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(STREAM_APP_1_OUT));

        final AtomicInteger counter = new AtomicInteger(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            System.out.println("Nb elements: " + counter.get());
        }));

        while (true) {
            final ConsumerRecords<String, Double> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Double> consumerRecord : consumerRecords) {
                counter.incrementAndGet();
                System.out.println(consumerRecord);
            }
        }
    }
}