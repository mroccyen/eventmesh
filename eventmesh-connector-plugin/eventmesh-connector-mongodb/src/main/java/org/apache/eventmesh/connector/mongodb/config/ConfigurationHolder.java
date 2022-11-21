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

package org.apache.eventmesh.connector.mongodb.config;

import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class ConfigurationHolder {
    private String url;

    private String database;

    private String collection;

    public void init() {
        String url = ConfigurationWrapper.getProperty(ConfigKey.URL);
        Preconditions.checkState(StringUtils.isNotEmpty(url), String.format("%s error", ConfigKey.URL));
        this.url = url;

        String database = ConfigurationWrapper.getProperty(ConfigKey.DATABASE);
        Preconditions.checkState(StringUtils.isNotEmpty(database), String.format("%s error", ConfigKey.DATABASE));
        this.database = database;

        String collection = ConfigurationWrapper.getProperty(ConfigKey.COLLECTION);
        Preconditions.checkState(StringUtils.isNotEmpty(collection), String.format("%s error", ConfigKey.COLLECTION));
        this.collection = collection;
    }
}
