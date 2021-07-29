package io.odpf.firehose.sink.mongodb.util;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

/**
 * The type Mongo sink factory util.
 */
@UtilityClass
public class MongoSinkFactoryUtil {


    /**
     * Log mongo config.
     *
     * @param mongoSinkConfig the mongo sink config
     * @param instrumentation the instrumentation
     */
    public static void logMongoConfig(MongoSinkConfig mongoSinkConfig, Instrumentation instrumentation) {
        String mongoConfig = String.format("\n\tMONGO connection urls: %s\n\tMONGO DB name: %s\n\tMONGO Primary Key: %s\n\tMONGO message type: %s"
                        + "\n\tMONGO Collection Name: %s\n\tMONGO request timeout in ms: %s\n\tMONGO retry status code blacklist: %s"
                        + "\n\tMONGO update only mode: %s"
                        + "\n\tMONGO should preserve proto field names: %s",
                mongoSinkConfig.getSinkMongoConnectionUrls(), mongoSinkConfig.getSinkMongoDBName(), mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                mongoSinkConfig.getSinkMongoCollectionName(), mongoSinkConfig.getSinkMongoRequestTimeoutMs(), mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist(),
                mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable(), true);
        instrumentation.logDebug(mongoConfig);

    }
}
