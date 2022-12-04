package org.apache.eventmesh.connector.mongodb.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.eventmesh.connector.mongodb.config.ConfigurationHolder;
import org.apache.eventmesh.connector.mongodb.constant.MongodbConstants;

public class MongodbSequenceUtil {
    private final MongoClient mongoClient;
    private final DB db;
    private final DBCollection seqCol;

    public MongodbSequenceUtil(ConfigurationHolder configurationHolder) {
        mongoClient = new MongoClient();
        db = mongoClient.getDB(configurationHolder.getDatabase());
        seqCol = db.getCollection(MongodbConstants.SEQUENCE_COLLECTION_NAME);
    }

    public int getNextSeq(String topic) {
        BasicDBObject query = new BasicDBObject(MongodbConstants.SEQUENCE_KEY_FN, topic);
        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(MongodbConstants.SEQUENCE_VALUE_FN, 1));
        BasicDBObject result = (BasicDBObject) seqCol.findAndModify(query, update);
        return (int) (Integer) result.get(MongodbConstants.SEQUENCE_VALUE_FN);
    }
}
