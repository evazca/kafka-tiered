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
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RemoteLogSegmentMetadataSerde implements RemoteLogMetadataSerdes<RemoteLogSegmentMetadata> {

    public Message serialize(RemoteLogSegmentMetadata data) {
        RemoteLogSegmentMetadataRecord record = new RemoteLogSegmentMetadataRecord()
                .setRemoteLogSegmentId(
                        new RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry()
                                .setTopicIdPartition(new RemoteLogSegmentMetadataRecord.TopicIdPartitionEntry()
                                        .setId(data.remoteLogSegmentId().topicIdPartition().topicId())
                                        .setName(data.remoteLogSegmentId().topicIdPartition().topicPartition()
                                                .topic())
                                        .setPartition(data.remoteLogSegmentId().topicIdPartition().topicPartition()
                                                .partition()))
                                .setId(data.remoteLogSegmentId().id()))
                .setStartOffset(data.startOffset())
                .setEndOffset(data.endOffset())
                .setBrokerId(data.brokerId())
                .setEventTimestampMs(data.eventTimestampMs())
                .setMaxTimestampMs(data.maxTimestampMs())
                .setSegmentSizeInBytes(data.segmentSizeInBytes())
                .setSegmentLeaderEpochs(data.segmentLeaderEpochs().entrySet().stream()
                        .map(entry -> new RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry()
                                .setLeaderEpoch(entry.getKey())
                                .setOffset(entry.getValue())).collect(Collectors.toList()))
                .setRemoteLogSegmentState(data.state().id());

        return record;
    }

    @Override
    public RemoteLogSegmentMetadata deserialize(byte version, ByteBuffer byteBuffer) {
        RemoteLogSegmentMetadataRecord record = new RemoteLogSegmentMetadataRecord(
                new ByteBufferAccessor(byteBuffer), version);

        RemoteLogSegmentId remoteLogSegmentId = buildRemoteLogSegmentId(record.remoteLogSegmentId());
        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        for (RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry segmentLeaderEpoch : record.segmentLeaderEpochs()) {
            segmentLeaderEpochs.put(segmentLeaderEpoch.leaderEpoch(), segmentLeaderEpoch.offset());
        }
        RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(remoteLogSegmentId,
                record.startOffset(), record.endOffset(), record.maxTimestampMs(), record.brokerId(),
                record.eventTimestampMs(), record.segmentSizeInBytes(), segmentLeaderEpochs);
        RemoteLogSegmentMetadataUpdate rlsmUpdate = new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId,
                record.eventTimestampMs(), RemoteLogSegmentState.forId(record.remoteLogSegmentState()),
                record.brokerId());

        return remoteLogSegmentMetadata.createWithUpdates(rlsmUpdate);
    }

    private RemoteLogSegmentId buildRemoteLogSegmentId(RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry entry) {
        TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id(),
                new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));
        return new RemoteLogSegmentId(topicIdPartition, entry.id());
    }
}
