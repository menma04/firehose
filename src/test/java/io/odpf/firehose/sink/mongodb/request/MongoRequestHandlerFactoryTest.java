package io.odpf.firehose.sink.mongodb.request;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.config.enums.MongoSinkRequestType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.serializer.MessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoRequestHandlerFactoryTest {

    @Mock
    private MongoSinkConfig mongoSinkConfig;

    @Mock
    private Instrumentation instrumentation;

    private MessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnMongoRequestHandler() {
        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(new Random().nextBoolean());
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, instrumentation, "id",
                MongoSinkMessageType.JSON, jsonSerializer);
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        assertEquals(MongoRequestHandler.class, requestHandler.getClass().getSuperclass());
    }

    @Test
    public void shouldReturnUpsertRequestHandler() {
        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(false);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, instrumentation, "id",
                MongoSinkMessageType.JSON, jsonSerializer);
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("Mongo request mode: {}", MongoSinkRequestType.UPSERT);
        assertEquals(MongoUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable()).thenReturn(true);
        MongoRequestHandlerFactory mongoRequestHandlerFactory = new MongoRequestHandlerFactory(mongoSinkConfig, instrumentation, "id",
                MongoSinkMessageType.JSON, jsonSerializer);
        MongoRequestHandler requestHandler = mongoRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("Mongo request mode: {}", MongoSinkRequestType.UPDATE_ONLY);
        assertEquals(MongoUpdateRequestHandler.class, requestHandler.getClass());
    }
}
