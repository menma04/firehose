package io.odpf.firehose.sink.mongodb.util;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

/**
 * The utility helper class for the MongoSinkFactory.
 *
 * @since 0.1
 */
@UtilityClass
public class MongoSinkFactoryUtil {

    /**
     * Logs all the configuration parameters of MongoDB Sink to the instrumentation
     * logger, in Debug Mode. If the parameter is null, i.e. not specified, then the
     * logger logs "null" to the log console.
     *
     * @param mongoSinkConfig the mongo sink config object
     * @param instrumentation the instrumentation containing the logger
     * @since 0.1
     */
    public static void logMongoConfig(MongoSinkConfig mongoSinkConfig, Instrumentation instrumentation) {
        String mongoConfig = String.format("\n\tMONGO connection urls: %s"
                        + "\n\tMONGO Database name: %s"
                        + "\n\tMONGO Primary Key: %s"
                        + "\n\tMONGO input message type: %s"
                        + "\n\tMONGO Collection Name: %s"
                        + "\n\tMONGO request timeout in ms: %s"
                        + "\n\tMONGO retry status code blacklist: %s"
                        + "\n\tMONGO update only mode: %s"
                        + "\n\tMONGO Authentication Enable: %s"
                        + "\n\tMONGO Authentication Username: %s"
                        + "\n\tMONGO Authentication Database: %s",

                mongoSinkConfig.getSinkMongoConnectionUrls(),
                mongoSinkConfig.getSinkMongoDBName(),
                mongoSinkConfig.getSinkMongoPrimaryKey(),
                mongoSinkConfig.getSinkMongoInputMessageType(),
                mongoSinkConfig.getSinkMongoCollectionName(),
                mongoSinkConfig.getSinkMongoRequestTimeoutMs(),
                mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist(),
                mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable(),
                mongoSinkConfig.isSinkMongoAuthEnable(),
                mongoSinkConfig.getSinkMongoAuthUsername(),
                mongoSinkConfig.getSinkMongoAuthDB());

        instrumentation.logDebug(mongoConfig);
    }
}
