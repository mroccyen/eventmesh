package org.apache.eventmesh.connector.mongodb.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.eventmesh.connector.mongodb.exception.MongodbConnectorException;
import org.bson.Document;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class MongodbCloudEventUtil {
    public static CloudEvent convertToCloudEvent(Document document) {
        String versionStr = document.getString("version");
        SpecVersion version = SpecVersion.parse(versionStr);
        CloudEventBuilder builder;
        switch (version) {
            case V03:
                builder = CloudEventBuilder.v03();
                break;
            case V1:
                builder = CloudEventBuilder.v1();
                break;
            default:
                throw new MongodbConnectorException(String.format("CloudEvent version %s does not support.", version));
        }
        builder.withData(document.remove("data").toString().getBytes())
                .withId(document.remove("id").toString())
                .withSource(URI.create(document.remove("source").toString()))
                .withType(document.remove("type").toString())
                .withDataContentType(document.remove("datacontenttype").toString())
                .withSubject(document.remove("subject").toString());
        document.forEach((key, value) -> builder.withExtension(key, value.toString()));

        return builder.build();
    }

    public static Document convertToDocument(CloudEvent cloudEvent) {
        Document document = new Document();
        document.put("version", cloudEvent.getSpecVersion().name());
        document.put("data", cloudEvent.getData() == null
                ? null : new String(cloudEvent.getData().toBytes(), StandardCharsets.UTF_8));
        document.put("id", cloudEvent.getId());
        document.put("source", cloudEvent.getSource().toString());
        document.put("type", cloudEvent.getType());
        document.put("datacontenttype", cloudEvent.getDataContentType());
        document.put("subject", cloudEvent.getSubject());
        cloudEvent.getExtensionNames().forEach(key -> document.put(key, cloudEvent.getExtension(key)));

        return document;
    }

    public static CloudEvent convertToCloudEvent(DBObject dbObject) {
        String versionStr = (String) dbObject.get("version");
        SpecVersion version = SpecVersion.parse(versionStr);
        CloudEventBuilder builder;
        switch (version) {
            case V03:
                builder = CloudEventBuilder.v03();
                break;
            case V1:
                builder = CloudEventBuilder.v1();
                break;
            default:
                throw new MongodbConnectorException(String.format("CloudEvent version %s does not support.", version));
        }
        builder.withData(dbObject.removeField("data").toString().getBytes())
                .withId(dbObject.removeField("id").toString())
                .withSource(URI.create(dbObject.removeField("source").toString()))
                .withType(dbObject.removeField("type").toString())
                .withDataContentType(dbObject.removeField("datacontenttype").toString())
                .withSubject(dbObject.removeField("subject").toString());
        dbObject.keySet().forEach(key -> builder.withExtension(key, dbObject.get(key).toString()));

        return builder.build();
    }

    public static BasicDBObject convertToDBObject(CloudEvent cloudEvent) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("version", cloudEvent.getSpecVersion().name());
        dbObject.put("data", cloudEvent.getData() == null
                ? null : new String(cloudEvent.getData().toBytes(), StandardCharsets.UTF_8));
        dbObject.put("id", cloudEvent.getId());
        dbObject.put("source", cloudEvent.getSource().toString());
        dbObject.put("type", cloudEvent.getType());
        dbObject.put("datacontenttype", cloudEvent.getDataContentType());
        dbObject.put("subject", cloudEvent.getSubject());
        cloudEvent.getExtensionNames().forEach(key -> dbObject.put(key, cloudEvent.getExtension(key)));

        return dbObject;
    }
}
