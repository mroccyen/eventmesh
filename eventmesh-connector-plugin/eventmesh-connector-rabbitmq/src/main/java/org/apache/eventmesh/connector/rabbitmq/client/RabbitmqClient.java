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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitmqClient {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqClient.class);

    /**
     * get rabbitmq connection
     *
     * @param host        host
     * @param username    username
     * @param passwd      password
     * @param port        port
     * @param virtualHost virtual host
     * @return connection
     * @throws Exception Exception
     */
    public Connection getConnection(String host, String username,
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

    /**
     * send message
     *
     * @param channel      channel
     * @param exchangeName exchange name
     * @param routingKey   routing key
     * @param message      message
     * @throws Exception Exception
     */
    public void publish(Channel channel, String exchangeName,
                        String routingKey, String message) throws Exception {
        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
    }

    /**
     * consume message
     *
     * @param channel             channel
     * @param builtinExchangeType exchange type
     * @param exchangeName        exchange name
     * @param routingKey          routing key
     * @param queueName           queue name
     * @param autoAck             true is auto ack
     * @param encoding            encoding
     */
    public void consume(Channel channel, BuiltinExchangeType builtinExchangeType,
                        String exchangeName, String routingKey, String queueName,
                        boolean autoAck, String encoding) {
        encoding = StringUtils.isNotBlank(encoding) ? encoding : "utf-8";
        try {
            channel.exchangeDeclare(exchangeName, builtinExchangeType.getType(), true,
                    false, false, null);
            channel.queueDeclare(queueName, false, false,
                    false, null);
            routingKey = builtinExchangeType.getType().equals(BuiltinExchangeType.FANOUT.getType()) ? "" : routingKey;
            channel.queueBind(queueName, exchangeName, routingKey);

            while (true) {
                GetResponse response = channel.basicGet(queueName, autoAck);
                if (response != null) {
                    logger.info(new String(response.getBody(), encoding));
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                }
            }
        } catch (Exception ex) {
            //todo log
        } finally {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception ex) {
                //todo log
            }
        }
    }

    /**
     * close connection
     *
     * @param connection connection
     */
    public void closeConnection(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (Exception ex) {
                //todo log
            }
        }
    }

    /**
     * close channel
     *
     * @param channel channel
     */
    public void closeChannel(Channel channel) {
        if (null != channel) {
            try {
                channel.close();
            } catch (Exception ex) {
                //todo log
            }
        }
    }
}
