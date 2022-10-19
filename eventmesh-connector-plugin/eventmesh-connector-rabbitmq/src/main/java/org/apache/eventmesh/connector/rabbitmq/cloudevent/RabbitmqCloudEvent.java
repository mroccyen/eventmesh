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

package org.apache.eventmesh.connector.rabbitmq.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;

import java.io.Serializable;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;

public class RabbitmqCloudEvent implements CloudEvent, Serializable {

    private final CloudEvent adapter;

    public RabbitmqCloudEvent(CloudEvent cloudEvent) {
        this.adapter = cloudEvent;
    }

    @Override
    public CloudEventData getData() {
        return adapter.getData();
    }

    @Override
    public SpecVersion getSpecVersion() {
        return adapter.getSpecVersion();
    }

    @Override
    public String getId() {
        return adapter.getId();
    }

    @Override
    public String getType() {
        return adapter.getType();
    }

    @Override
    public URI getSource() {
        return adapter.getSource();
    }

    @Override
    public String getDataContentType() {
        return adapter.getDataContentType();
    }

    @Override
    public URI getDataSchema() {
        return adapter.getDataSchema();
    }

    @Override
    public String getSubject() {
        return adapter.getSubject();
    }

    @Override
    public OffsetDateTime getTime() {
        return adapter.getTime();
    }

    @Override
    public Object getAttribute(String attributeName) throws IllegalArgumentException {
        return adapter.getAttribute(attributeName);
    }

    @Override
    public Object getExtension(String extensionName) {
        return adapter.getExtension(extensionName);
    }

    @Override
    public Set<String> getExtensionNames() {
        return adapter.getExtensionNames();
    }
}
