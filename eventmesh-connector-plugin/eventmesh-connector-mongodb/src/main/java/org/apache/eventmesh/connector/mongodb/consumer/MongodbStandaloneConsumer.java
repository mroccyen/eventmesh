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

import com.mongodb.*;
import io.cloudevents.CloudEvent;
import org.apache.eventmesh.api.AbstractContext;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.consumer.Consumer;
import org.apache.eventmesh.common.ThreadPoolFactory;
import org.apache.eventmesh.connector.mongodb.client.MongodbClientStandaloneManager;
import org.apache.eventmesh.connector.mongodb.config.ConfigurationHolder;
import org.apache.eventmesh.connector.mongodb.constant.MongodbConstants;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongodbStandaloneConsumer implements Consumer {

    private final ConfigurationHolder configurationHolder;

    private volatile boolean started = false;

    private EventListener eventListener;

    private MongoClient client;

    private DB db;

    private DBCollection cappedCol;

    private final ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, Runtime.getRuntime().availableProcessors() * 2, "EventMesh-Mongodb-Consumer-");

    public MongodbStandaloneConsumer(ConfigurationHolder configurationHolder) {
        this.configurationHolder = configurationHolder;
    }

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
                MongodbClientStandaloneManager.closeMongodbClient(this.client);
            } finally {
                started = false;
            }
        }
    }

    @Override
    public void init(Properties keyValue) {
        this.configurationHolder.init();
        this.client = MongodbClientStandaloneManager.createMongodbClient(configurationHolder);
        this.db = client.getDB(configurationHolder.getDatabase());
        this.cappedCol = db.getCollection(configurationHolder.getCollection());
    }

    @Override
    public void updateOffset(List<CloudEvent> cloudEvents, AbstractContext context) {

    }

    @Override
    public void subscribe(String topic) {
        executor.execute(new SubTask());
    }

    @Override
    public void unsubscribe(String topic) {

    }

    @Override
    public void registerEventListener(EventListener listener) {
        this.eventListener = listener;
    }

    private DBCursor getCursor(DBCollection collection, String topic, int lastId) {
        DBObject options = new BasicDBObject()
                .append(MongodbConstants.CAPPED_COL_OPTION_TAILABLE_FN, true)
                .append(MongodbConstants.CAPPED_COL_OPTION_AWAIT_DATA_FN, true)
                .append(MongodbConstants.CAPPED_COL_TOPIC_FN, true)
                .append(MongodbConstants.CAPPED_COL_NAME_FN, true)
                .append(MongodbConstants.CAPPED_COL_CURSOR_FN, true);

        DBObject index = new BasicDBObject("$gt", lastId);
        BasicDBObject ts = new BasicDBObject(MongodbConstants.CAPPED_COL_CURSOR_FN, index);

        DBObject spec = ts.append(MongodbConstants.CAPPED_COL_TOPIC_FN, topic);
        DBCursor cur = collection.find(spec, options);
        cur = cur.addOption(8);
        return cur;
    }

    private class SubTask implements Runnable {
        private final AtomicBoolean stop = new AtomicBoolean(false);

        public void run() {
            int lastId = -1;
            while (!stop.get()) {
                DBCursor cur = getCursor(cappedCol, MongodbConstants.Topic, lastId);
                Iterator<DBObject> it = cur.iterator();
                while (it.hasNext()) {
                    DBObject obj = it.next();
                    System.out.println("name is:" + obj.get(MongodbConstants.CAPPED_COL_NAME_FN));
                    try {
                        lastId = (int) ((Double) obj.get(MongodbConstants.CAPPED_COL_CURSOR_FN)).doubleValue();
                    } catch (ClassCastException ce) {
                        lastId = (Integer) obj.get(MongodbConstants.CAPPED_COL_CURSOR_FN);
                    }
                    System.out.println("last index is:" + lastId);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            stop.set(true);
        }
    }
}
