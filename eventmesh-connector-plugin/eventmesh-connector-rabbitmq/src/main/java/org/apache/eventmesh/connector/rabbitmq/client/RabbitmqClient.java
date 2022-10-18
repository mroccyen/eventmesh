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

package org.apache.eventmesh.connector.rabbitmq.client;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.StringUtils;

public class RabbitmqClient {
    /**
     * get rabbitmq connection
     *
     * @param host        host
     * @param username    user name
     * @param passwd      password
     * @param port        port
     * @param virtualHost virtual host
     * @return connection
     * @throws Exception Exception
     */
    public static Connection getConnection(String host, String username,
                                           String passwd, int port,
                                           String virtualHost) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host.trim());
        factory.setPort(port);
        virtualHost = virtualHost.trim().startsWith("/") ? virtualHost : "/" + virtualHost;
        if (StringUtils.isNotEmpty(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }
        factory.setUsername(username);
        factory.setPassword(passwd.trim());

        return factory.newConnection();
    }


    public static void sendMessage(Channel channel, String exchange, String queue, String message) throws Exception {
        channel.queueDeclare(queue, false, false, false, null);
        channel.basicPublish(exchange, queue, null, message.getBytes());
    }
}
