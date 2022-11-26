package org.apache.eventmesh.connector.mongodb.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.eventmesh.connector.mongodb.constant.MongodbConstants;

public class MongodbSequenceUtil {
    private final static MongodbSequenceUtil sequence = new MongodbSequenceUtil();
    private MongoClient mongoClient;
    private DB db;
    private DBCollection seqCol;

    public static synchronized MongodbSequenceUtil getInstance() {
        return sequence;
    }

    private MongodbSequenceUtil() {
        mongoClient = new MongoClient();
        db = mongoClient.getDB("");
        seqCol = db.getCollection(MongodbConstants.SEQUENCE_COLLECTION_NAME);
    }

    public int getNextSeq(String topic) {
        BasicDBObject query = new BasicDBObject(MongodbConstants.SEQUENCE_KEY_FN, topic);
        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(MongodbConstants.SEQUENCE_VALUE_FN, 1));
        BasicDBObject result = (BasicDBObject) seqCol.findAndModify(query, update);
        int value = ((Integer) result.get(MongodbConstants.SEQUENCE_VALUE_FN)).intValue();
        return value;
    }
}
