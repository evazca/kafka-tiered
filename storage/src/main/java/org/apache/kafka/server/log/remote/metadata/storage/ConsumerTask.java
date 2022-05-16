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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 * <p>
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogMetadataTopicPartitioner#metadataPartition(TopicIdPartition)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that are deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);
    static long pollIntervalMs = 100L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final Consumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler handler;
    private final RemoteLogMetadataTopicPartitioner partitioner;

    private volatile boolean isClosed = false;
    // It indicates whether the consumer needs to assign the partitions or not. This is set when it is
    // determined that the consumer needs to be assigned with the updated partitions.
    private volatile boolean isAssignmentChanged = true;
    private final Object assignmentLock = new Object();

    private volatile Set<Integer> assignedMetadataPartitions = Collections.emptySet();
    // User topic partitions that this broker is a leader/follower for.
    private volatile Map<TopicIdPartition, UserTopicIdPartition> assignedUserTopicIdPartitions = Collections.emptyMap();
    private boolean isAllUserTopicPartitionsInitialized;

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> readOffsetsByMetadataPartition = new ConcurrentHashMap<>();
    private final Map<TopicIdPartition, Long> readOffsetsByUserTopicPartition = new HashMap<>();

    private Map<TopicPartition, Long> endOffsetsByMetadataPartition = new HashMap<>();
    private boolean isEndOffsetsFetchFailed = false;

    public ConsumerTask(final RemotePartitionMetadataEventHandler handler,
                        final RemoteLogMetadataTopicPartitioner partitioner,
                        final Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
        this.handler = Objects.requireNonNull(handler);
        this.partitioner = Objects.requireNonNull(partitioner);
        this.consumer = consumerSupplier.get();
    }

    @Override
    public void run() {
        log.info("Starting consumer task thread.");
        try {
            while (!isClosed) {
                if (isAssignmentChanged) {
                    maybeWaitForPartitionAssignment();
                }
                final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(pollIntervalMs));
                if (!consumerRecords.isEmpty()) {
                    log.debug("Processing {} records", consumerRecords.count());
                    for (final ConsumerRecord<byte[], byte[]> record: consumerRecords) {
                        final RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
                        if (canProcess(remoteLogMetadata, record.offset())) {
                            handler.handleRemoteLogMetadata(remoteLogMetadata);
                            readOffsetsByUserTopicPartition.put(remoteLogMetadata.topicIdPartition(), record.offset());
                        }
                        readOffsetsByMetadataPartition.put(record.partition(), record.offset());
                    }
                }
                maybeMarkUserPartitionsAsReady();
            }
        } catch (final WakeupException ex) {
            // ignore
        } catch (final Exception e) {
            log.error("Error occurred while processing the records", e);
        } finally {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (final Exception e) {
                log.error("Error encountered while closing the consumer", e);
            }
        }
        log.info("Exited from consumer task thread");
    }

    private void maybeMarkUserPartitionsAsReady() {
        if (isAllUserTopicPartitionsInitialized) {
            return;
        }
        maybeFetchEndOffsets();
        boolean isAllInitialized = true;
        for (final UserTopicIdPartition utp : assignedUserTopicIdPartitions.values()) {
            if (!utp.isInitialized) {
                final Integer metadataPartition = utp.metadataPartition;
                final Long endOffset = endOffsetsByMetadataPartition.get(toRemoteLogPartition(metadataPartition));
                // The end-offset can be null, when the recent assignment wasn't picked up by the consumer.
                if (endOffset != null) {
                    final Long readOffset = readOffsetsByMetadataPartition.getOrDefault(metadataPartition, -1L);
                    // The end-offset was fetched only once during reassignment. The metadata-partition can
                    // receive new stream of records, so the consumer can read records more than the last-fetched end-offset.
                    if (readOffset + 1 >= endOffset) {
                        markInitialized(utp);
                    }
                }
            }
            isAllInitialized = isAllInitialized && utp.isInitialized;
        }
        isAllUserTopicPartitionsInitialized = isAllInitialized;
    }

    private boolean canProcess(final RemoteLogMetadata metadata, final long recordOffset) {
        final TopicIdPartition tpId = metadata.topicIdPartition();
        final Long readOffset = readOffsetsByUserTopicPartition.get(tpId);
        return assignedUserTopicIdPartitions.containsKey(tpId) && (readOffset == null || readOffset < recordOffset);
    }

    private void maybeWaitForPartitionAssignment() throws InterruptedException {
        final Set<Integer> metadataPartitionSnapshot = new HashSet<>();
        synchronized (assignmentLock) {
            while (!isClosed && assignedUserTopicIdPartitions.isEmpty()) {
                log.debug("Waiting for remote log metadata partitions to be assigned");
                assignmentLock.wait();
            }
            if (!isClosed && isAssignmentChanged) {
                assignedUserTopicIdPartitions.values().forEach(utp -> metadataPartitionSnapshot.add(utp.metadataPartition));
                isAssignmentChanged = false;
            }
        }
        if (!metadataPartitionSnapshot.isEmpty()) {
            // FIXME: For the unassigned user-partitions,
            //          - close the remote-log-metadata cache for the user-partition and
            //          - remove the corresponding partition offset from `readOffsetsByUserTopicPartition` map
            final Map<TopicPartition, Long> currentPosition = consumer.assignment()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), consumer::position));
            final Set<TopicPartition> remoteLogPartitions = toRemoteLogPartitions(metadataPartitionSnapshot);
            consumer.assign(remoteLogPartitions);
            this.assignedMetadataPartitions = Collections.unmodifiableSet(metadataPartitionSnapshot);
            // for newly assigned user-partitions, read from the beginning of the corresponding metadata partition
            final Set<TopicPartition> seekToBeginOffsetPartitions = assignedUserTopicIdPartitions.values()
                    .stream()
                    .filter(utp -> !utp.isAssigned)
                    .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                    .collect(Collectors.toSet());
            consumer.seekToBeginning(seekToBeginOffsetPartitions);
            // for other metadata partitions, read from the offset where the processing left last time.
            remoteLogPartitions.stream()
                    .filter(tp -> !seekToBeginOffsetPartitions.contains(tp))
                    .forEach(tp -> consumer.seek(tp, currentPosition.get(tp)));
            // mark all the user-topic-partitions as assigned to the consumer.
            assignedUserTopicIdPartitions.values().forEach(utp -> utp.isAssigned = true);
            isAllUserTopicPartitionsInitialized = false;
            fetchEndOffsets();
        }
    }

    public void addAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Objects.requireNonNull(partitions), Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Collections.emptySet(), Objects.requireNonNull(partitions));
    }

    private void updateAssignments(final Set<TopicIdPartition> addedPartitions,
                                   final Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for partitions added: {} and removed: {}", addedPartitions, removedPartitions);
        if (!addedPartitions.isEmpty() || !removedPartitions.isEmpty()) {
            synchronized (assignmentLock) {
                final Map<TopicIdPartition, UserTopicIdPartition> idealUserPartitions = new HashMap<>(assignedUserTopicIdPartitions);
                addedPartitions.forEach(tpId -> idealUserPartitions.putIfAbsent(tpId, newUserTopicIdPartition(tpId)));
                removedPartitions.forEach(idealUserPartitions::remove);
                if (!idealUserPartitions.equals(assignedUserTopicIdPartitions)) {
                    assignedUserTopicIdPartitions = Collections.unmodifiableMap(idealUserPartitions);
                    isAssignmentChanged = true;
                }
                if (isAssignmentChanged) {
                    log.debug("Assigned user-topic-partitions: {}", assignedUserTopicIdPartitions);
                    assignmentLock.notifyAll();
                }
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(final int partition) {
        return Optional.ofNullable(readOffsetsByMetadataPartition.get(partition));
    }

    public boolean isMetadataPartitionAssigned(final int partition) {
        return assignedMetadataPartitions.contains(partition);
    }

    public boolean isUserPartitionAssigned(final TopicIdPartition partition) {
        final UserTopicIdPartition utp = assignedUserTopicIdPartitions.get(partition);
        return utp != null && utp.isAssigned;
    }

    @Override
    public void close() {
        if (!isClosed) {
            log.info("Closing the instance");
            synchronized (assignmentLock) {
                isClosed = true;
                assignedUserTopicIdPartitions.values().forEach(this::markInitialized);
                consumer.wakeup();
                assignmentLock.notifyAll();
            }
        }
    }

    private void fetchEndOffsets() {
        try {
            final Set<TopicPartition> unInitializedPartitions = assignedUserTopicIdPartitions.values().stream()
                    .filter(utp -> utp.isAssigned && !utp.isInitialized)
                    .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                    .collect(Collectors.toSet());
            if (!unInitializedPartitions.isEmpty()) {
                endOffsetsByMetadataPartition = consumer.endOffsets(unInitializedPartitions);
            }
            isEndOffsetsFetchFailed = false;
        } catch (final TimeoutException ex) {
            // ignore LEADER_NOT_AVAILABLE error, this can happen when the partition leader is not yet assigned.
            isEndOffsetsFetchFailed = true;
        }
    }

    private void maybeFetchEndOffsets() {
        if (isEndOffsetsFetchFailed) {
            fetchEndOffsets();
        }
    }

    private UserTopicIdPartition newUserTopicIdPartition(final TopicIdPartition tpId) {
        return new UserTopicIdPartition(tpId, partitioner.metadataPartition(tpId));
    }

    private void markInitialized(final UserTopicIdPartition utp) {
        if (!utp.isInitialized) {
            handler.markInitialized(utp.topicIdPartition);
            utp.isInitialized = true;
        }
    }

    static Set<TopicPartition> toRemoteLogPartitions(final Set<Integer> partitions) {
        return partitions.stream()
                .map(ConsumerTask::toRemoteLogPartition)
                .collect(Collectors.toSet());
    }

    static TopicPartition toRemoteLogPartition(int partition) {
        return new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition);
    }

    static class UserTopicIdPartition {
        private final TopicIdPartition topicIdPartition;
        private final Integer metadataPartition;
        // The `utp` will be initialized once it reads all the existing events from the remote log metadata topic.
        boolean isInitialized;
        // denotes whether this `utp` is assigned to the consumer
        boolean isAssigned;

        /**
         * UserTopicIdPartition denotes the user topic-partitions for which this broker acts as a leader/follower of.
         *
         * @param tpId               the unique topic partition identifier
         * @param metadataPartition  the remote log metadata partition mapped for this user-topic-partition.
         */
        public UserTopicIdPartition(final TopicIdPartition tpId, final Integer metadataPartition) {
            this.topicIdPartition = Objects.requireNonNull(tpId);
            this.metadataPartition = Objects.requireNonNull(metadataPartition);
            this.isInitialized = false;
            this.isAssigned = false;
        }

        @Override
        public String toString() {
            return "UserTopicIdPartition{" +
                    "topicIdPartition=" + topicIdPartition +
                    ", metadataPartition=" + metadataPartition +
                    ", isInitialized=" + isInitialized +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserTopicIdPartition that = (UserTopicIdPartition) o;
            return topicIdPartition.equals(that.topicIdPartition) && metadataPartition.equals(that.metadataPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, metadataPartition);
        }
    }
}
