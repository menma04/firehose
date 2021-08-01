package io.odpf.firehose.sink.mongodb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.config.enums.SinkType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.serializer.MessageToJson;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.mongodb.client.MongoSinkClient;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandlerFactory;
import io.odpf.firehose.sink.mongodb.util.MongoSinkFactoryUtil;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Sink factory to configure and create MongoDB sink.
 */
public class MongoSinkFactory implements SinkFactory {

    /**
     * Creates MongoDB sink. Logs a success message to instrumentation
     * upon successful creation of the sink.
     *
     * @param configuration  the configuration map
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return created sink
     * @since 0.1
     */
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, MongoSinkFactory.class);

        MongoSinkFactoryUtil.logMongoConfig(mongoSinkConfig, instrumentation);
        MongoRequestHandler mongoRequestHandler = new MongoRequestHandlerFactory(mongoSinkConfig, new Instrumentation(statsDReporter, MongoRequestHandlerFactory.class),
                mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                new MessageToJson(new ProtoParser(stencilClient, mongoSinkConfig.getInputSchemaProtoClass()), true, false)

        ).getRequestHandler();

        MongoSinkClient mongoSinkClient = new MongoSinkClient(mongoSinkConfig, instrumentation);

        instrumentation.logInfo("MONGO connection established");
        return new MongoSink(new Instrumentation(statsDReporter, MongoSink.class), SinkType.MONGODB.name().toLowerCase(), mongoRequestHandler,
                mongoSinkClient);
    }
}
