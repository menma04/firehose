package io.odpf.firehose.sink.mongodb.util;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

/**
 * The utility helper class for the MongoSinkFactory.
 */
@UtilityClass
public class MongoSinkFactoryUtil {

    /**
     * Logs all the configuration parameters of MongoDB Sink to the instrumentation logger,
     * in Debug Mode.
     *
     * @param mongoSinkConfig the mongo sink config object
     * @param instrumentation the instrumentation containing the logger
     * @throws NullPointerException if any of the parameters of MongoSinkConfig
     *                              is null, i.e. if the user does not specify the value of the
     *                              corresponding environment variable before launching Firehose.
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
