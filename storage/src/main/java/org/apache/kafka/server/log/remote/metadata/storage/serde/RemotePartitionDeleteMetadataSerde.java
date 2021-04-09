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
package org.apache.kafka.server.log.remote.metadata.storage.serde;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;

import java.nio.ByteBuffer;

public final class RemotePartitionDeleteMetadataSerde implements RemoteLogMetadataSerdes<RemotePartitionDeleteMetadata> {

    @Override
    public Message serialize(RemotePartitionDeleteMetadata data) {
        RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord()
                .setTopicIdPartition(new RemotePartitionDeleteMetadataRecord.TopicIdPartitionEntry()
                        .setName(data.topicIdPartition().topicPartition().topic())
                        .setPartition(data.topicIdPartition().topicPartition().partition())
                        .setId(data.topicIdPartition().topicId()))
                .setEventTimestampMs(data.eventTimestampMs())
                .setBrokerId(data.brokerId())
                .setRemotePartitionDeleteState(data.state().id());
        return record;
    }

    public RemotePartitionDeleteMetadata deserialize(byte version, ByteBuffer byteBuffer) {
        RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord(
                new ByteBufferAccessor(byteBuffer), version);
        TopicIdPartition topicIdPartition = new TopicIdPartition(record.topicIdPartition().id(),
                new TopicPartition(record.topicIdPartition().name(), record.topicIdPartition().partition()));

        return new RemotePartitionDeleteMetadata(topicIdPartition,
                RemotePartitionDeleteState.forId(record.remotePartitionDeleteState()),
                record.eventTimestampMs(), record.brokerId());
    }
}
