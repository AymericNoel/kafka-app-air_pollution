package collection.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import io.github.cdimascio.dotenv.Dotenv;
import javax.net.ssl.SSLContext;

import java.util.Arrays;
import java.util.List;

class Collector {
    private static String API_KEY; 
    private static final String OPEN_DATA_URL_PARIS = "https://api.waqi.info/feed/paris/?token=" ;
    private static final String OPEN_DATA_URL_BEIJING = "https://api.waqi.info/feed/beijing/?token=" ;
    private static final String OPEN_DATA_URL_NEWYORK = "https://api.waqi.info/feed/newyork/?token=" ;
    private static final String OPEN_DATA_URL_LONDON = "https://api.waqi.info/feed/london/?token=" ;


    private static final List<String> ALL_URL = Arrays.asList(OPEN_DATA_URL_BEIJING, OPEN_DATA_URL_PARIS, OPEN_DATA_URL_NEWYORK, OPEN_DATA_URL_LONDON);

    private static final Logger logger = LoggerFactory.getLogger(Collector.class);

    private final KafkaPublisher publisher;

    Collector(KafkaPublisher publisher) {
        this.publisher = publisher;
        Dotenv dotenv = Dotenv.load();
        API_KEY = dotenv.get("API_KEY");
    }

    void collect() {
        try {

            TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                    NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslsf)
                            .register("http", new PlainConnectionSocketFactory())
                            .build();

            BasicHttpClientConnectionManager connectionManager =
                    new BasicHttpClientConnectionManager(socketFactoryRegistry);
            CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                    .setConnectionManager(connectionManager).build();

            HttpComponentsClientHttpRequestFactory requestFactory =
                    new HttpComponentsClientHttpRequestFactory(httpClient);
                    
            for (String url : ALL_URL) {
                url=url+API_KEY;
                ResponseEntity<String> response =
                    new RestTemplate(requestFactory).exchange(url, HttpMethod.GET, null, String.class);    

                String jsonString = response.getBody();

                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(jsonString);
                JsonNode data = jsonNode.get("data");
                publisher.publish(data.toString());                
            }            

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}