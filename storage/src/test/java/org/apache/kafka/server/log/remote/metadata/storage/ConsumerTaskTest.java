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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.log.remote.metadata.storage.ConsumerTask.UserTopicIdPartition;
import static org.apache.kafka.server.log.remote.metadata.storage.ConsumerTask.toRemoteLogPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ConsumerTaskTest {

    private final int numMetadataTopicPartitions = 5;
    private final RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(numMetadataTopicPartitions);
    private final DummyEventHandler handler = new DummyEventHandler();
    private final Set<TopicPartition> remoteLogPartitions = IntStream.range(0, numMetadataTopicPartitions).boxed()
            .map(ConsumerTask::toRemoteLogPartition).collect(Collectors.toSet());
    private final Uuid topicId = Uuid.randomUuid();
    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

    private ConsumerTask consumerTask;
    private MockConsumer<byte[], byte[]> consumer;
    private Thread thread;

    @BeforeEach
    public void beforeEach() {
        final Map<TopicPartition, Long> offsets = remoteLogPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> 0L));
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(offsets);
        ConsumerTask.pollIntervalMs = 10L;
        consumerTask = new ConsumerTask(handler, partitioner, () -> consumer);
        thread = new Thread(consumerTask);
    }

    @AfterEach
    public void afterEach() throws InterruptedException {
        if (thread != null) {
            consumerTask.close();
            thread.join();
        }
    }

    @Test
    public void testCloseOnNoAssignment() throws InterruptedException {
        thread.start();
        Thread.sleep(10);
    }

    @Test
    public void testIdempotentClose() {
        consumerTask.close();
        consumerTask.close();
    }

    @Test
    public void testUserTopicIdPartitionEquals() {
        final TopicIdPartition tpId = new TopicIdPartition(topicId, new TopicPartition("sample", 0));
        final UserTopicIdPartition utp1 = new UserTopicIdPartition(tpId, partitioner.metadataPartition(tpId));
        final UserTopicIdPartition utp2 = new UserTopicIdPartition(tpId, partitioner.metadataPartition(tpId));
        utp1.isInitialized = true;
        utp1.isAssigned = true;

        assertFalse(utp2.isInitialized);
        assertFalse(utp2.isAssigned);
        assertEquals(utp1, utp2);
    }

    @Test
    public void testAddAssignmentsForPartitions() throws InterruptedException {
        final List<TopicIdPartition> idPartitions = getIdPartitions("sample", 3);
        final Map<TopicPartition, Long> endOffsets = idPartitions.stream()
                .map(idp -> toRemoteLogPartition(partitioner.metadataPartition(idp)))
                .collect(Collectors.toMap(Function.identity(), e -> 0L, (a, b) -> b));
        consumer.updateEndOffsets(endOffsets);
        consumerTask.addAssignmentsForPartitions(new HashSet<>(idPartitions));
        thread.start();
        for (final TopicIdPartition idPartition : idPartitions) {
            TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(idPartition), 1000, "");
            assertTrue(consumerTask.isMetadataPartitionAssigned(partitioner.metadataPartition(idPartition)));
        }
    }

    @Test
    public void testRemoveAssignmentsForPartitions() throws InterruptedException {
        final List<TopicIdPartition> allPartitions = getIdPartitions("sample", 3);
        final Map<TopicPartition, Long> endOffsets = allPartitions.stream()
                .map(idp -> toRemoteLogPartition(partitioner.metadataPartition(idp)))
                .collect(Collectors.toMap(Function.identity(), e -> 0L, (a, b) -> b));
        consumer.updateEndOffsets(endOffsets);
        consumerTask.addAssignmentsForPartitions(new HashSet<>(allPartitions));
        thread.start();

        final TopicIdPartition removedTopicIdPartition = allPartitions.get(0);
        final Set<TopicIdPartition> removePartitions = new HashSet<>();
        removePartitions.add(removedTopicIdPartition);
        consumerTask.removeAssignmentsForPartitions(removePartitions);
        for (final TopicIdPartition idPartition : allPartitions) {
            final TestCondition condition = () -> removePartitions.contains(idPartition) == !consumerTask.isUserPartitionAssigned(idPartition);
            TestUtils.waitForCondition(condition, 1000, "");
        }
    }

    @Test
    public void testCanProcessRecord() throws InterruptedException {
        final Uuid topicId = Uuid.fromString("Bp9TDduJRGa9Q5rlvCJOxg");
        final TopicIdPartition tpId0 = new TopicIdPartition(topicId, new TopicPartition("sample", 0));
        final TopicIdPartition tpId1 = new TopicIdPartition(topicId, new TopicPartition("sample", 1));
        final TopicIdPartition tpId2 = new TopicIdPartition(topicId, new TopicPartition("sample", 2));
        assert partitioner.metadataPartition(tpId0) == partitioner.metadataPartition(tpId1);
        assert partitioner.metadataPartition(tpId0) == partitioner.metadataPartition(tpId2);

        final int metadataPartition = partitioner.metadataPartition(tpId0);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 0L));
        final Set<TopicIdPartition> assignments = new HashSet<>(Collections.singletonList(tpId0));
        consumerTask.addAssignmentsForPartitions(assignments);
        thread.start();
        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId0), 1000, "");

        addRecord(consumer, metadataPartition, tpId0, 0);
        addRecord(consumer, metadataPartition, tpId0, 1);
        TestUtils.waitForCondition(() -> consumerTask.receivedOffsetForPartition(metadataPartition).equals(Optional.of(1L)), 1000, "Couldn't read record");
        assertEquals(2, handler.metadataCounter);

        // should only read the tpId1 records
        consumerTask.addAssignmentsForPartitions(new HashSet<>(Collections.singletonList(tpId1)));
        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId1), 1000, "");
        addRecord(consumer, metadataPartition, tpId1, 2);
        TestUtils.waitForCondition(() -> consumerTask.receivedOffsetForPartition(metadataPartition).equals(Optional.of(2L)), 1000, "Couldn't read record");
        assertEquals(3, handler.metadataCounter);

        // shouldn't read tpId2 records
        addRecord(consumer, metadataPartition, tpId2, 3);
        TestUtils.waitForCondition(() -> consumerTask.receivedOffsetForPartition(metadataPartition).equals(Optional.of(3L)), 1000, "Couldn't read record");
        assertEquals(3, handler.metadataCounter);
    }

    @Test
    public void testMaybeMarkUserPartitionsAsReady() throws InterruptedException {
        final TopicIdPartition tpId = getIdPartitions("hello", 1).get(0);
        final int metadataPartition = partitioner.metadataPartition(tpId);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 2L));
        consumerTask.addAssignmentsForPartitions(new HashSet<>(Collections.singletonList(tpId)));
        thread.start();

        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), 1000, "");
        assertTrue(consumerTask.isMetadataPartitionAssigned(metadataPartition));
        assertFalse(handler.isPartitionInitialized.containsKey(tpId));
        IntStream.range(0, 5).forEach(offset -> addRecord(consumer, metadataPartition, tpId, offset));
        TestUtils.waitForCondition(() -> consumerTask.receivedOffsetForPartition(metadataPartition).equals(Optional.of(4L)), 1000, "Couldn't read records");
        assertTrue(handler.isPartitionInitialized.get(tpId));
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        thread.start();
        final CountDownLatch latch = new CountDownLatch(1);
        final TopicIdPartition tpId = getIdPartitions("concurrent", 1).get(0);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(partitioner.metadataPartition(tpId)), 0L));
        final Thread assignmentThread = new Thread(() -> {
            try {
                latch.await();
                consumerTask.addAssignmentsForPartitions(new HashSet<>(Collections.singletonList(tpId)));
            } catch (final InterruptedException e) {
                fail("Shouldn't have thrown an exception");
            }
        });
        final Thread closeThread = new Thread(() -> {
            try {
                latch.await();
                consumerTask.close();
            } catch (final InterruptedException e) {
                fail("Shouldn't have thrown an exception");
            }
        });
        assignmentThread.start();
        closeThread.start();

        latch.countDown();
        assignmentThread.join();
        closeThread.join();
    }

    private void addRecord(final MockConsumer<byte[], byte[]> consumer,
                           final int metadataPartition,
                           final TopicIdPartition idPartition,
                           final long recordOffset) {
        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(idPartition, Uuid.randomUuid());
        final RemoteLogMetadata metadata = new RemoteLogSegmentMetadata(segmentId, 0L, 1L, 0L, 0, 0L, 1, Collections.singletonMap(0, 0L));
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME, metadataPartition, recordOffset, null, serde.serialize(metadata));
        consumer.addRecord(record);
    }

    private List<TopicIdPartition> getIdPartitions(final String topic, final int partitionCount) {
        final List<TopicIdPartition> idPartitions = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            idPartitions.add(new TopicIdPartition(topicId, new TopicPartition(topic, partition)));
        }
        return idPartitions;
    }

    private static class DummyEventHandler extends RemotePartitionMetadataEventHandler {
        private int metadataCounter = 0;
        private final Map<TopicIdPartition, Boolean> isPartitionInitialized = new HashMap<>();

        @Override
        protected void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            metadataCounter++;
        }

        @Override
        protected void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
        }

        @Override
        protected void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
        }

        @Override
        public void syncLogMetadataSnapshot(TopicIdPartition topicIdPartition, int metadataPartition, Long metadataPartitionOffset) {
        }

        @Override
        public void clearTopicPartition(TopicIdPartition topicIdPartition) {
        }

        @Override
        public void markInitialized(TopicIdPartition partition) {
            isPartitionInitialized.put(partition, true);
        }
    }
}