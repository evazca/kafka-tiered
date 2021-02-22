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
package org.apache.kafka.common;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * This represents universally unique identifier with topic id for a topic partition. This makes sure that topics
 * recreated with the same name will always have unique topic identifiers.
 */
public class TopicIdPartition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID topicId;
    private final TopicPartition topicPartition;

    public TopicIdPartition(UUID topicId,
                            TopicPartition topicPartition) {
        Objects.requireNonNull(topicId, "topicId can not be null");
        Objects.requireNonNull(topicPartition, "topicPartition can not be null");
        this.topicId = topicId;
        this.topicPartition = topicPartition;
    }

    /**
     * @return Universally unique id representing this topic partition.
     */
    public UUID topicId() {
        return topicId;
    }

    /**
     * @return Topic partition representing this instance.
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicIdPartition that = (TopicIdPartition) o;
        return Objects.equals(topicId, that.topicId) &&
               Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, topicPartition);
    }

    @Override
    public String toString() {
        return "TopicIdPartition{" +
               "topicId=" + topicId +
               ", topicPartition=" + topicPartition +
               '}';
    }
}
