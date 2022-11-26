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

package org.apache.eventmesh.connector.mongodb.client;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.connector.mongodb.config.ConfigurationHolder;

import java.net.InetSocketAddress;

public class MongodbClientStandaloneManager {
    /**
     * create mongodb client
     *
     * @param config ConfigurationHolder
     * @return mongodb client
     */
    public static MongoClient createMongodbClient(ConfigurationHolder config) {
        String url = config.getUrl();
        String[] split = url.split(":");
        InetSocketAddress address = new InetSocketAddress(split[0], Integer.parseInt(split[1]));
        ServerAddress serverAddress = new ServerAddress(address);
        if (StringUtils.isNotEmpty(config.getUsername()) && StringUtils.isNotEmpty(config.getPassword())) {
            MongoCredential credential = MongoCredential.createCredential(config.getUsername(), config.getDatabase(), config.getPassword().toCharArray());
            return new MongoClient(serverAddress, credential, null);
        } else {
            return new MongoClient(serverAddress);
        }
    }

    /**
     * close mongodb client
     *
     * @param mongoClient mongodb client
     */
    public static void closeMongodbClient(MongoClient mongoClient) {
        mongoClient.close();
    }
}
