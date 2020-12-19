package analysis.mean_AQI;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static config.KafkaConfig.BOOTSTRAP_SERVERS;
import static analysis.mean_AQI.producer.STREAM_APP_2_OUT;
import static config.KafkaConfig.GROUP_ID;

public class consumer {

    public static void main(String[] args) {

        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        final Consumer<WindowedSerdes.TimeWindowedSerde<String>, Double> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(STREAM_APP_2_OUT));

        final AtomicInteger counter = new AtomicInteger(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            System.out.println("Nb elements: " + counter.get());
        }));

        while (true) {
            final ConsumerRecords<WindowedSerdes.TimeWindowedSerde<String>, Double> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<WindowedSerdes.TimeWindowedSerde<String>, Double> consumerRecord : consumerRecords) {
                counter.incrementAndGet();
                System.out.println(consumerRecord);
            }
        }
    }

}
