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

package org.apache.eventmesh.connector.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.cloudevents.CloudEvent;
import org.apache.eventmesh.api.RequestReplyCallback;
import org.apache.eventmesh.api.SendCallback;
import org.apache.eventmesh.api.SendResult;
import org.apache.eventmesh.api.exception.ConnectorRuntimeException;
import org.apache.eventmesh.api.exception.OnExceptionContext;
import org.apache.eventmesh.api.producer.Producer;
import org.apache.eventmesh.connector.rabbitmq.client.RabbitmqClient;

import java.util.Properties;

public class RabbitmqProducer implements Producer {

    private RabbitmqClient rabbitmqClient;

    private Connection connection;

    private Channel channel;

    private volatile boolean started = false;

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
                rabbitmqClient.closeConnection(connection);
                rabbitmqClient.closeChannel(channel);
            } finally {
                started = false;
            }
        }
    }

    @Override
    public void init(Properties properties) throws Exception {
        this.rabbitmqClient = new RabbitmqClient();
        this.connection = rabbitmqClient.getConnection("", "", "", 0, "");
        this.channel = connection.createChannel();
    }

    @Override
    public void publish(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
        try {
            rabbitmqClient.publish(channel, "", "", "");

            SendResult sendResult = new SendResult();
            sendResult.setTopic(cloudEvent.getSubject());
            sendResult.setMessageId(cloudEvent.getId());
            sendCallback.onSuccess(sendResult);
        } catch (Exception ex) {
            sendCallback.onException(
                    OnExceptionContext.builder()
                            .topic(cloudEvent.getSubject())
                            .messageId(cloudEvent.getId())
                            .exception(new ConnectorRuntimeException(ex))
                            .build()
            );
        }
    }

    @Override
    public void sendOneway(CloudEvent cloudEvent) {
        try {
            rabbitmqClient.publish(channel, "", "", "");
        } catch (Exception ex) {
            //todo log
        }
    }

    @Override
    public void request(CloudEvent cloudEvent, RequestReplyCallback rrCallback, long timeout) throws Exception {

    }

    @Override
    public boolean reply(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
        return false;
    }

    @Override
    public void checkTopicExist(String topic) throws Exception {

    }

    @Override
    public void setExtFields() {

    }
}
