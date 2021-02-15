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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public class RLMSerDesTest {

    @Test
    public void testSerDes() throws Exception {
        final String topic = "foo";

        RemoteLogSegmentMetadata rlsm1 = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(UUID.randomUUID(), new TopicPartition("bar", 0))
                        , UUID.randomUUID()),
                1000L,
                2000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentState.COPY_SEGMENT_STARTED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsm1);

        RemoteLogSegmentMetadata rlsm2 = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(UUID.randomUUID(), new TopicPartition("bar", 0))
                        , UUID.randomUUID()),
                2000L,
                4000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsm2);

        //RLSM marked with deletion
        RemoteLogSegmentMetadata rlsmMarkedDelete = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(UUID.randomUUID(), new TopicPartition("bar", 0))
                        , UUID.randomUUID()),
                2000L,
                4000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentState.DELETE_SEGMENT_STARTED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsmMarkedDelete);

    }

    private void doTestSerDes(final String topic, final RemoteLogSegmentMetadata rlsm) {
        try (RLMSerDe rlsmSerDe = new RLMSerDe()) {
            rlsmSerDe.configure(Collections.emptyMap(), false);

            final Serializer<RemoteLogSegmentMetadata> serializer = rlsmSerDe.serializer();
            final byte[] serializedBytes = serializer.serialize(topic, rlsm);

            final Deserializer<RemoteLogSegmentMetadata> deserializer = rlsmSerDe.deserializer();
            final RemoteLogSegmentMetadata deserializedRlsm = deserializer.deserialize(topic, serializedBytes);

            Assert.assertEquals(rlsm, deserializedRlsm);
        }
    }
}
