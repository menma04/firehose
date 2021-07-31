package io.odpf.firehose.sink.mongodb.util;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkFactoryUtilTest {

    private MongoSinkConfig mongoSinkConfig;
    private HashMap<String, String> config = new HashMap<>();

    @Mock
    private Instrumentation instrumentation;


    @Before
    public void setup() {
        initMocks(this);

        config.put("SINK_MONGO_CONNECTION_URLS", "localhost:8080");
        config.put("SINK_MONGO_DB_NAME", "sampleDatabase");
        config.put("SINK_MONGO_PRIMARY_KEY", "customer_id");
        config.put("SINK_MONGO_INPUT_MESSAGE_TYPE", "JSON");
        config.put("SINK_MONGO_COLLECTION_NAME", "customers");
        config.put("SINK_MONGO_REQUEST_TIMEOUT_MS", "8277");
        config.put("SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST", "11000");
        config.put("SINK_MONGO_MODE_UPDATE_ONLY_ENABLE", "true");
        config.put("SINK_MONGO_AUTH_ENABLE", "false");

    }


    @Test
    public void shouldLogMongoSinkConfig() {
        mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, config);

        MongoSinkFactoryUtil.logMongoConfig(mongoSinkConfig, instrumentation);
        verify(instrumentation, times(1)).logDebug(any());

    }

    @Test
    public void shouldLogMongoSinkConfigWithCorrectMessageWhenAuthDisabled() {
        mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, config);

        MongoSinkFactoryUtil.logMongoConfig(mongoSinkConfig, instrumentation);
        verify(instrumentation, times(1))
                .logDebug("\n\tMONGO connection urls: localhost:8080"
                        + "\n\tMONGO Database name: sampleDatabase"
                        + "\n\tMONGO Primary Key: customer_id"
                        + "\n\tMONGO input message type: JSON"
                        + "\n\tMONGO Collection Name: customers"
                        + "\n\tMONGO request timeout in ms: 8277"
                        + "\n\tMONGO retry status code blacklist: 11000"
                        + "\n\tMONGO update only mode: true"
                        + "\n\tMONGO Authentication Enable: false"
                        + "\n\tMONGO Authentication Username: null"
                        + "\n\tMONGO Authentication Database: null");

    }

    @Test
    public void shouldLogMongoSinkConfigWithCorrectMessageWhenAuthEnabled() {

        config.put("SINK_MONGO_AUTH_ENABLE", "true");
        config.put("SINK_MONGO_AUTH_USERNAME", "john");
        config.put("SINK_MONGO_AUTH_DB", "agents");

        mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, config);

        MongoSinkFactoryUtil.logMongoConfig(mongoSinkConfig, instrumentation);
        verify(instrumentation, times(1))
                .logDebug("\n\tMONGO connection urls: localhost:8080"
                        + "\n\tMONGO Database name: sampleDatabase"
                        + "\n\tMONGO Primary Key: customer_id"
                        + "\n\tMONGO input message type: JSON"
                        + "\n\tMONGO Collection Name: customers"
                        + "\n\tMONGO request timeout in ms: 8277"
                        + "\n\tMONGO retry status code blacklist: 11000"
                        + "\n\tMONGO update only mode: true"
                        + "\n\tMONGO Authentication Enable: true"
                        + "\n\tMONGO Authentication Username: john"
                        + "\n\tMONGO Authentication Database: agents");

    }
}
