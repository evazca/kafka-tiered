/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.UUID;

/**
 * This represents a universally unique associated to a topic partition's log segment. This will be regenerated for
 * every attempt of copying a specific log segment in {@link RemoteLogStorageManager#copyLogSegment(RemoteLogSegmentId, LogSegmentData)}.
 */
public class RemoteLogSegmentId {
    private TopicPartition topicPartition;
    private UUID id;

    public RemoteLogSegmentId(TopicPartition topicPartition, UUID id) {
        this.topicPartition = requireNonNull(topicPartition);
        this.id = requireNonNull(id);
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public UUID id() {
        return id;
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentId{" +
                "topicPartition=" + topicPartition +
                ", id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogSegmentId that = (RemoteLogSegmentId) o;
        return Objects.equals(topicPartition, that.topicPartition) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, id);
    }
}
