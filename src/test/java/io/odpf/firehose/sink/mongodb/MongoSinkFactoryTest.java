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

        configuration.put("SINK_MONGO_DB_NAME", "myDb");
        configuration.put("SINK_MONGO_COLLECTION_NAME", "sampleCollection");
    }

    @Test
    public void shouldCreateMongoSink() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:9200 , localhost:9200 ");

        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        Sink sink = mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(MongoSink.class, sink.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenServerURLsInvalid() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:qfb");
        thrown.expect(IllegalArgumentException.class);

        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
    }

    @Test
    public void shouldThrowExceptionWhenServerURLPortNotSpecified() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("SINK_MONGO_CONNECTION_URLS should contain host and port both");
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
    }


    @Test
    public void shouldThrowExceptionWhenServerURLsEmpty() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("SINK_MONGO_CONNECTION_URLS is empty or null");

        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
    }
}
