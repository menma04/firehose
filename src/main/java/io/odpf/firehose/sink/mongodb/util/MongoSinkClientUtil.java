package io.odpf.firehose.sink.mongodb.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Mongo sink factory util.
 */
@UtilityClass
public class MongoSinkClientUtil {

    /**
     * Extracts the MongoDB Server URLs from the connection URLs string and converts the
     * URL of each MongoDB server to a ServerAddress object and then stores these addresses
     * of all the MongoDB servers into a list, which is returned.
     *
     * @param mongoConnectionUrls the mongo connection urls
     * @param instrumentation     the instrumentation
     * @return the list of server addresses
     * @throws IllegalArgumentException if the environment variable SINK_MONGO_CONNECTION_URLS
     * is an empty string or not assigned any value by the user.
     * This exception is also thrown if the URL does not contain
     * any or both of hostname/ IP address and the port
     *
     */
    public static List<ServerAddress> getServerAddresses(String mongoConnectionUrls, Instrumentation instrumentation) {
        if (mongoConnectionUrls != null && !mongoConnectionUrls.isEmpty()) {
            List<String> mongoNodes = Arrays.asList(mongoConnectionUrls.trim().split(","));
            List<ServerAddress> serverAddresses = new ArrayList<>(mongoNodes.size());
            mongoNodes.forEach((String mongoNode) -> {
                List<String> node = Arrays.stream(mongoNode.trim().split(":"))
                        .filter(nodeString -> !nodeString.isEmpty()).collect(Collectors.toList());
                if (node.size() <= 1) {
                    throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS should contain host and port both");
                }
                serverAddresses.add(new ServerAddress(node.get(0).trim(), Integer.parseInt(node.get(1).trim())));
            });
            return serverAddresses;
        } else {
            instrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS is empty or null");
        }
    }

    /**
     * Gets status codes as list.
     *
     * @param mongoRetryStatusCodeBlacklist the mongo retry status code blacklist
     * @return the status codes as list
     */
    public static List<String> getStatusCodesAsList(String mongoRetryStatusCodeBlacklist) {
        return Arrays
                .stream(mongoRetryStatusCodeBlacklist.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Builds the Mongo client.
     *
     * This method extracts the MongoDB Server URL and port from the MongoSinkConfig.
     * Multiple server seeds are also allowed to connect the MongoClient
     *
     * Then, this method checks whether the parameter SINK_MONGO_AUTH_ENABLE is true or not
     * If Authentication parameter is enabled then it extracts the login credentials, i.e.
     * username, password and the MongoDB authentication database.
     * If Authentication parameter is disabled then the MongoClient session is started
     * in non-authentication mode.
     *
     *
     * @param mongoSinkConfig the mongo sink config
     * @param instrumentation the instrumentation
     * @return the mongo client
     */
    public static MongoClient buildMongoClient(MongoSinkConfig mongoSinkConfig, Instrumentation instrumentation) {
        List<ServerAddress> serverAddresses = getServerAddresses(mongoSinkConfig.getSinkMongoConnectionUrls(), instrumentation);
        MongoClientOptions options = MongoClientOptions.builder().connectTimeout(mongoSinkConfig.getSinkMongoRequestTimeoutMs()).build();

        MongoClient mongoClient;
        if (mongoSinkConfig.isSinkMongoAuthEnable()) {
            MongoCredential mongoCredential = MongoCredential.createCredential(mongoSinkConfig.getSinkMongoAuthUsername(), mongoSinkConfig.getSinkMongoAuthDB(), mongoSinkConfig.getSinkMongoAuthPassword().toCharArray());
            mongoClient = new MongoClient(serverAddresses, mongoCredential, options);
        } else {
            mongoClient = new MongoClient(serverAddresses, options);
        }
        return mongoClient;
    }
}
