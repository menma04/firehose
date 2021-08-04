package io.odpf.firehose.sink.mongodb;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        initMocks(this);

        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:8080");
        configuration.put("SINK_MONGO_DB_NAME", "sampleDatabase");
        configuration.put("SINK_MONGO_PRIMARY_KEY", "customer_id");
        configuration.put("SINK_MONGO_INPUT_MESSAGE_TYPE", "JSON");
        configuration.put("SINK_MONGO_COLLECTION_NAME", "customers");
        configuration.put("SINK_MONGO_REQUEST_TIMEOUT_MS", "8277");
        configuration.put("SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST", "11000");
        configuration.put("SINK_MONGO_MODE_UPDATE_ONLY_ENABLE", "true");
        configuration.put("SINK_MONGO_AUTH_ENABLE", "false");

    }

    @Test
    public void shouldCreateMongoSink() {

        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        Sink sink = mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(MongoSink.class, sink.getClass());
    }
}
