package config;

import java.util.Collections;
import java.util.List;

public class KafkaConfig {

    public static final List<String> BOOTSTRAP_SERVERS = Collections.singletonList("192.168.99.100:9092");

    public static final String RAW_TOPIC_NAME = "pollution-stats-raw";

    public static final String API_KEY = "e41c9a07343541812f7b8fd408441388e07c00c2";

    public static final String GROUP_ID = "pollution-group";
}
