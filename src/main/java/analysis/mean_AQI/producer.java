package analysis.mean_AQI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import java.time.Duration;

import java.io.IOException;
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

        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> stream = builder.stream(STREAM_APP_2_INPUT);

        // KStream<String, String> airQualityStream = stats
        //         .selectKey((key, jsonRecordString) -> extract_city(jsonRecordString))
        //         .map((key, value) -> new KeyValue<>(key, extract_pollution_meter(value)));

        // airQualityStream.to(STREAM_APP_1_OUT, Produced.with(stringSerde, stringSerde));
        class Tuple2<T1, T2> {
            public T1 value1;
            public T2 value2;
            
            Tuple2(T1 v1, T2 v2) {
              value1 = v1;
              value2 = v2;
            }
        }

        KGroupedStream<String, Double> groupedStream =
                stream.groupByKey();

        KTable<Windowed<String>, Double> timeWindowedAggregatedStream =
                groupedStream
                        .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                        
                        .aggregate(
                                () -> 0L,
                                (aggKey, newValue, aggValue) -> aggValue + newValue,
                                Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                                        .withValueSerde(Serdes.Double()));

        KTable<Windowed<String>,Tuple2<Long,Double>> countAndSum = 
                groupedStream
                        .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                        .aggregate(
                            new Initializer<Tuple2<Long, Double>>() {
                                @Override
                                public Tuple2<Long, Double> apply() {
                                    return new Tuple2<>(0L, 0.0d);
                                }
                            },
                            new Aggregator<String, String, Tuple2<Long, Double>>() {
                                @Override
                                public Tuple2<Long, Double> apply(final String key, final Long value, final Tuple2<Long, Double> aggregate) {
                                        ++aggregate.value1;
                                        aggregate.value2 += value;
                                        return aggregate;
                                }
                            },
                            new Tuple2Serde());

        KTable<Long, Double> Average =
        
                timeWindowedAggregatedStream.mapValues(value -> value / ,
                                        Materialized.as("average-ratings"));

                                        
        timeWindowedAggregatedStream.toStream().to("velib-nbfreedocks-count-notifications", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));
        // final KTable<Long, CountAndSum> ratingCountAndSum =
        // ratingsById.aggregate(() -> new CountAndSum(0L, 0.0),
        //                       (key, value, aggregate) -> {
        //                         aggregate.setCount(aggregate.getCount() + 1);
        //                         aggregate.setSum(aggregate.getSum() + value);
        //                         return aggregate;
        //                       },
        //                       Materialized.with(Long(), countAndSumSerde));
        
        // KTable<Long, Double> ratingAverage =
        //         ratingCountAndSum.mapValues(value -> value.getSum() / value.getCount(),
        //                                 Materialized.as("average-ratings"));
        return builder.build();
    }

    // final KStreamBuilder builder = new KStreamBuilder();

    // // first step: compute count and sum in a single aggregation step and emit
    // // 2-tuples <count,sum> as aggregation result values
    // final KTable<String, Tuple2<Long, Long>> countAndSum = builder.stream("someInputTopic").groupByKey()
    //         .aggregate(new Initializer<Tuple2<Long, Long>>() {
    //             @Override
    //             public Tuple2<Long, Long> apply() {
    //                 return new Tuple2<>(0L, 0L);
    //             }
    //         }, new Aggregator<String, String, Tuple2<Long, Long>>() {
    //             @Override
    //             public Tuple2<Long, Long> apply(final String key, final Long value,
    //                     final Tuple2<Long, Long> aggregate) {
    //                 ++aggregate.value1;
    //                 aggregate.value2 += value;
    //                 return aggregate;
    //             }
    //         }, new Tuple2Serde()); // omitted for brevity
    // // second step: compute average for each 2-tuple
    // final KTable<String, Double> average = countAndSum.mapValues(new ValueMapper<Tuple2<Long, Long>, Double>() {
    //     @Override
    //     public Double apply(Tuple2<Long, Long> value) {
    //         return value.value2 / (double) value.value1;
    //     }
    // });
    // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Stream+Usage+Patterns

}
