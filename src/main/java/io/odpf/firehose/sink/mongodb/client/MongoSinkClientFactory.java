package io.odpf.firehose.sink.mongodb.client;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.mongodb.util.MongoSinkClientFactoryUtil;
import lombok.AllArgsConstructor;
import org.bson.Document;

import java.util.List;

@AllArgsConstructor
public class MongoSinkClientFactory {

    private final MongoSinkConfig mongoSinkConfig;
    private final Instrumentation instrumentation;

    public MongoSinkClient create() {

        MongoClient mongoClient = MongoSinkClientFactoryUtil.buildMongoClient(mongoSinkConfig, instrumentation);
        MongoDatabase database = mongoClient.getDatabase(mongoSinkConfig.getSinkMongoDBName());
        MongoCollection<Document> collection = database.getCollection(mongoSinkConfig.getSinkMongoCollectionName());

        List<String> mongoRetryStatusCodeBlacklist = MongoSinkClientFactoryUtil.getStatusCodesAsList(mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist());
        instrumentation.logInfo("MONGO connection established");
        return new MongoSinkClient(collection, instrumentation, mongoRetryStatusCodeBlacklist, mongoClient);
    }
}
