/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.remote.metadata.storage;

import java.util.Objects;

/**
 *
 */
public class RemoteLogMetadataContext {
    private final byte apiKey;
    private final byte version;
    private final Object payload;

    public RemoteLogMetadataContext(byte apiKey, byte version, Object payload) {
        this.apiKey = apiKey;
        this.version = version;
        this.payload = payload;
    }

    public byte apiKey() {
        return apiKey;
    }

    public byte version() {
        return version;
    }

    public Object payload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogMetadataContext that = (RemoteLogMetadataContext) o;
        return apiKey == that.apiKey && version == that.version && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiKey, version, payload);
    }

    @Override
    public String toString() {
        return "RemoteLogMetdataContext{" +
                "apiKey=" + apiKey +
                ", version=" + version +
                ", payload=" + payload +
                '}';
    }
}