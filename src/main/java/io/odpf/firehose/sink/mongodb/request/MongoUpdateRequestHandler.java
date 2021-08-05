package io.odpf.firehose.sink.mongodb.request;

import com.mongodb.client.model.ReplaceOneModel;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.config.enums.MongoSinkRequestType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.serializer.MessageToJson;
import org.bson.Document;
import org.json.simple.JSONObject;

/**
 * The Mongo update request handler.
 * This class is responsible for creating requests when one
 * or more fields of a MongoDB document need to be updated.
 */
public class MongoUpdateRequestHandler extends MongoRequestHandler {

    private final MongoSinkRequestType mongoSinkRequestType;
    private final String mongoPrimaryKey;

    /**
     * Instantiates a new Mongo update request handler.
     *
     * @param messageType          the message type
     * @param jsonSerializer       the json serializer
     * @param mongoSinkRequestType the mongo sink request type
     * @param mongoPrimaryKey      the mongo primary key
     * @since 0.1
     */
    public MongoUpdateRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey, String kafkaRecordParserMode) {
        super(messageType, jsonSerializer, kafkaRecordParserMode);
        this.mongoSinkRequestType = mongoSinkRequestType;
        this.mongoPrimaryKey = mongoPrimaryKey;
    }

    @Override
    public boolean canCreate() {
        return mongoSinkRequestType == MongoSinkRequestType.UPDATE_ONLY;
    }

    @Override
    public ReplaceOneModel<Document> getRequest(Message message) {
        String logMessage = extractPayload(message);
        JSONObject logMessageJSONObject = getJSONObject(logMessage);
        String primaryKeyValue;

        try {
            primaryKeyValue = getFieldFromJSON(logMessageJSONObject, mongoPrimaryKey);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().equals("Key cannot be null")) {
                throw new IllegalArgumentException("Primary Key cannot be null in Update-Only mode");
            } else {
                throw new IllegalArgumentException("Primary Key: " + mongoPrimaryKey + " was not found in ESB message");
            }
        }
        Document document = new Document("_id", primaryKeyValue);
        document.putAll(logMessageJSONObject);

        return new ReplaceOneModel<>(new Document("_id", primaryKeyValue), document);
    }
}
