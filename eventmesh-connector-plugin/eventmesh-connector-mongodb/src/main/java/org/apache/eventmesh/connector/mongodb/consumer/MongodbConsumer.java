/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eventmesh.connector.mongodb.consumer;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;

import org.apache.eventmesh.api.AbstractContext;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.EventMeshAction;
import org.apache.eventmesh.api.EventMeshAsyncConsumeContext;
import org.apache.eventmesh.api.consumer.Consumer;
import org.apache.eventmesh.common.ThreadPoolFactory;
import org.apache.eventmesh.connector.mongodb.client.MongodbClientManager;
import org.apache.eventmesh.connector.mongodb.config.ConfigurationHolder;

import org.apache.eventmesh.connector.mongodb.utils.MongodbCloudEventUtil;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class MongodbConsumer implements Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongodbConsumer.class);

    private final ConfigurationHolder configurationHolder = new ConfigurationHolder();

    private MongoClient mongoClient;

    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;

    private volatile boolean started = false;

    private EventListener eventListener;

    private final ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2,
            Runtime.getRuntime().availableProcessors() * 2,
            "EventMesh-Mongodb-Consumer-");

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isClosed() {
        return !isStarted();
    }

    @Override
    public void start() {
        if (!started) {
            started = true;
        }
    }

    @Override
    public void shutdown() {
        if (started) {
            try {
                if (this.mongoClient != null) {
                    MongodbClientManager.closeMongodbClient(this.mongoClient);
                }
                if (this.cursor != null) {
                    this.cursor.close();
                }
            } finally {
                started = false;
            }
        }
    }

    @Override
    public void init(Properties keyValue) {
        this.configurationHolder.init();
        this.mongoClient = MongodbClientManager.createMongodbClient(configurationHolder.getUrl());
    }

    @Override
    public void updateOffset(List<CloudEvent> cloudEvents, AbstractContext context) {

    }

    @Override
    public void subscribe(String topic) {
        MongoCollection<Document> collection = mongoClient
                .getDatabase(configurationHolder.getDatabase()).getCollection(configurationHolder.getCollection());
        ChangeStreamIterable<Document> changeStreamDocuments = collection.watch();
        this.cursor = changeStreamDocuments.cursor();
        this.handle();
    }

    @Override
    public void unsubscribe(String topic) {
        this.cursor.close();
    }

    @Override
    public void registerEventListener(EventListener listener) {
        this.eventListener = listener;
    }

    private void handle() {
        while (this.cursor.hasNext()) {
            ChangeStreamDocument<Document> next = cursor.next();
            Document fullDocument = next.getFullDocument();
            if (fullDocument != null) {
                CloudEvent cloudEvent = MongodbCloudEventUtil.convertToCloudEvent(fullDocument);
                final EventMeshAsyncConsumeContext consumeContext = new EventMeshAsyncConsumeContext() {
                    @Override
                    public void commit(EventMeshAction action) {
                        LOGGER.info("[MongodbConsumer] Mongodb consumer context commit.");
                    }
                };
                if (eventListener != null) {
                    eventListener.consume(cloudEvent, consumeContext);
                }
            }
        }
    }
}
