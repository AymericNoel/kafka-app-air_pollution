package analysis.pollution_per_city;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Properties;

import static config.KafkaConfig.RAW_TOPIC_NAME;
import static config.KafkaConfig.BOOTSTRAP_SERVERS;

class producer {

    static final String STREAM_APP_1_OUT = "pollution-per-city";
    private static final String POLLUTION_STREAM_APPLICATION = "pollutionCity-stream-application";
    private static final String STREAM_APP_1_INPUT = RAW_TOPIC_NAME;

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

        KStream<String, String> stats = builder.stream(STREAM_APP_1_INPUT);
        KStream<String, String> airQualityStream = stats
                .selectKey((key, jsonRecordString) -> extract_city(jsonRecordString))
                .map((key, value) -> new KeyValue<>(key, extract_pollution_meter(value)));

        airQualityStream.to(STREAM_APP_1_OUT, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private String extract_city(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode city = jsonNode.get("city");

        JsonNode cityName = city.get("name");

        return cityName.asText();
    }

    private String extract_pollution_meter(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode airQualityInfo = jsonNode.get("aqi");

        return airQualityInfo.asText();
    }
}
