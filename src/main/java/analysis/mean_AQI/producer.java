package analysis.mean_AQI;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import java.time.Duration;

import java.util.Map;
import java.util.Properties;

import static analysis.pollution_per_city.producer.STREAM_APP_1_OUT;

import static config.KafkaConfig.BOOTSTRAP_SERVERS;

class producer {

    static final String STREAM_APP_2_OUT = "mean_AQI";
    private static final String POLLUTION_STREAM_APPLICATION = "mean_aqi-stream-application";
    private static final String STREAM_APP_2_INPUT = STREAM_APP_1_OUT;

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, POLLUTION_STREAM_APPLICATION);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        producer airQualityApp = new producer();

        KafkaStreams streams = new KafkaStreams(airQualityApp.createTopology(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            streams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private Topology createTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> stream = builder.stream(STREAM_APP_2_INPUT);

        KGroupedStream<String, Double> groupedStream = stream.groupByKey();

        KTable<Windowed<String>, CountAndSum> timeWindowedAggregatedStream = groupedStream
                .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))

                .aggregate(() -> new CountAndSum(0L, 0.0), (aggKey, newValue, aggValue) -> {
                    aggValue.count = (aggValue.count + 1);
                    aggValue.sum = aggValue.sum + newValue;
                    return aggValue;
                }, Materialized.<String, CountAndSum, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                        .withValueSerde(CustomSerdes.CountAndSum())); // need to implement custom serde

        KTable<Windowed<String>, Double> Average = timeWindowedAggregatedStream
                .mapValues(value -> value.sum / value.count);

        Average.toStream().to(STREAM_APP_2_OUT,
                Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Double()));
        return builder.build();
    }
}

class CountAndSum {
    public long count;
    public double sum;

    CountAndSum(long v1, double v2) {
        count = v1;
        sum = v2;
    }
}

class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }

}

class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    public JsonDeserializer() {
    }

    public JsonDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {
        // nothing to do
    }
}

final class CustomSerdes {

    static public final class CountAndSumSerde
            extends Serdes.WrapperSerde<CountAndSum> {
        public CountAndSumSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(CountAndSum.class));
        }
    }

    public static Serde<CountAndSum> CountAndSum() {
        return new CustomSerdes.CountAndSumSerde();
    }

}