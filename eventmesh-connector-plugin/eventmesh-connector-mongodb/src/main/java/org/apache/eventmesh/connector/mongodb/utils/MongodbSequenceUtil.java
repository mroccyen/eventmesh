package org.apache.eventmesh.connector.mongodb.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;
import org.apache.eventmesh.connector.mongodb.client.MongodbClientStandaloneManager;
import org.apache.eventmesh.connector.mongodb.config.ConfigurationHolder;
import org.apache.eventmesh.connector.mongodb.constant.MongodbConstants;

@SuppressWarnings("all")
public class MongodbSequenceUtil {
    private final MongoClient mongoClient;
    private final DB db;
    private final DBCollection seqCol;

    public MongodbSequenceUtil(ConfigurationHolder configurationHolder) {
        mongoClient = MongodbClientStandaloneManager.createMongodbClient(configurationHolder);
        db = mongoClient.getDB(configurationHolder.getDatabase());
        seqCol = db.getCollection(MongodbConstants.SEQUENCE_COLLECTION_NAME);
    }

    public int getNextSeq(String topic) {
        BasicDBObject query = new BasicDBObject(MongodbConstants.SEQUENCE_KEY_FN, topic);
        BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject(MongodbConstants.SEQUENCE_VALUE_FN, 1));
        DBCollectionFindAndModifyOptions options = new DBCollectionFindAndModifyOptions();
        options.update(update);
        options.returnNew(true);
        options.upsert(true);
        BasicDBObject result = (BasicDBObject) seqCol.findAndModify(query, options);
        return (int) (Integer) result.get(MongodbConstants.SEQUENCE_VALUE_FN);
    }
}
