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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.RemoteLogUtils.metadataPartitionFor;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link TopicBasedRLMMConfig#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 * <p>
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogUtils#metadataPartitionFor(TopicIdPartition, int)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that ard deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);

    private static final long POLL_INTERVAL_MS = 30L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler;
    private final int noOfMetadataTopicPartitions;

    private volatile boolean close = false;
    private volatile boolean assignPartitions = false;

    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private Set<TopicIdPartition> assignedTopicPartitions = Collections.emptySet();

    // Map of remote log metadata topic partition to target end offsets to be consumed.
    private final Map<Integer, Long> partitionToTargetEndOffsets = new ConcurrentHashMap<>();

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> partitionToConsumedOffsets = new ConcurrentHashMap<>();

    public ConsumerTask(KafkaConsumer<byte[], byte[]> consumer,
                        RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                        int noOfMetadataTopicPartitions) {
        this.consumer = consumer;
        this.remotePartitionMetadataEventHandler = remotePartitionMetadataEventHandler;
        this.noOfMetadataTopicPartitions = noOfMetadataTopicPartitions;
    }

    @Override
    public void run() {
        log.info("Started Consumer task thread.");
        try {
            while (!close) {
                Set<Integer> assignedMetaPartitionsSnapshot = maybeWaitForPartitionsAssignment();

                if (!assignedMetaPartitionsSnapshot.isEmpty()) {
                    executeReassignment(assignedMetaPartitionsSnapshot);
                }

                log.info("Polling consumer to receive remote log metadata topic records");
                ConsumerRecords<byte[], byte[]> consumerRecords
                        = consumer.poll(Duration.ofSeconds(POLL_INTERVAL_MS));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    handleRemoteLogMetadata(serde.deserialize(record.value()));
                    partitionToConsumedOffsets.put(record.partition(), record.offset());
                }

                // Check whether messages are received till end offsets or not for the assigned metadata partitions.
                if (!partitionToTargetEndOffsets.isEmpty()) {
                    for (Map.Entry<Integer, Long> entry : partitionToTargetEndOffsets.entrySet()) {
                        final Long offset = partitionToConsumedOffsets.getOrDefault(entry.getKey(), 0L);
                        if (offset >= entry.getValue()) {
                            partitionToTargetEndOffsets.remove(entry.getKey());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error occurred in consumer task, close:[{}]", close, e);
        }

        closeConsumer();
        log.info("Exiting from consumer task thread");
    }

    private void closeConsumer() {
        log.info("Closing the consumer instance");
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                log.error("Error encountered while closing the consumer", e);
            }
        }
    }

    private Set<Integer> maybeWaitForPartitionsAssignment() {
        Set<Integer> assignedMetaPartitionsSnapshot = Collections.emptySet();
        synchronized (assignPartitionsLock) {
            while (assignedMetaPartitions.isEmpty()) {
                // If no partitions are assigned, wait until they are assigned.
                log.info("Waiting for assigned remote log metadata partitions..");
                try {
                    assignPartitionsLock.wait();
                } catch (InterruptedException e) {
                    throw new KafkaException(e);
                }
            }

            if (assignPartitions) {
                assignedMetaPartitionsSnapshot = new HashSet<>(assignedMetaPartitions);
                assignPartitions = false;
            }
        }
        return assignedMetaPartitionsSnapshot;
    }

    private void handleRemoteLogMetadata(RemoteLogMetadata remoteLogMetadata) {
        remotePartitionMetadataEventHandler.handleRemoteLogMetadata(remoteLogMetadata);
    }

    private void executeReassignment(Set<Integer> assignedMetaPartitionsSnapshot) {
        Set<TopicPartition> assignedMetaTopicPartitions = assignedMetaPartitionsSnapshot.stream()
                .map(partitionNum -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partitionNum))
                .collect(Collectors.toSet());
        log.info("Reassigning partitions to consumer task [{}]", assignedMetaTopicPartitions);
        consumer.assign(assignedMetaTopicPartitions);

        log.debug("Fetching end offsets to consumer task [{}]", assignedMetaTopicPartitions);
        Map<TopicPartition, Long> endOffsets;
        while (true) {
            try {
                endOffsets = consumer.endOffsets(assignedMetaTopicPartitions, Duration.ofSeconds(30));
                break;
            } catch (Exception e) {
                // ignore exception
                log.debug("Error encountered in fetching end offsets", e);
            }
        }
        log.debug("Fetched end offsets to consumer task [{}]", endOffsets);

        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (entry.getValue() > 0) {
                partitionToTargetEndOffsets.put(entry.getKey().partition(), entry.getValue());
            }
        }
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> updatedPartitions) {
        updateAssignmentsForPartitions(updatedPartitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(Collections.emptySet(), partitions);
    }

    private void updateAssignmentsForPartitions(Set<TopicIdPartition> addedPartitions,
                                                Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for addedPartitions: {} and removedPartition: {}", addedPartitions, removedPartitions);
        ensureNotClosed();

        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");

        if (addedPartitions.isEmpty() && removedPartitions.isEmpty()) {
            return;
        }

        synchronized (assignPartitionsLock) {
            Set<TopicIdPartition> updatedReassignedPartitions = new HashSet<>(assignedTopicPartitions);
            updatedReassignedPartitions.addAll(addedPartitions);
            updatedReassignedPartitions.removeAll(removedPartitions);
            Set<Integer> updatedAssignedMetaPartitions = new HashSet<>();
            for (TopicIdPartition tp : updatedReassignedPartitions) {
                updatedAssignedMetaPartitions.add(metadataPartitionFor(tp, noOfMetadataTopicPartitions));
            }

            if (!updatedAssignedMetaPartitions.equals(assignedMetaPartitions)) {
                assignedTopicPartitions = Collections.unmodifiableSet(updatedReassignedPartitions);
                assignedMetaPartitions = Collections.unmodifiableSet(updatedAssignedMetaPartitions);
                assignPartitions = true;
                assignPartitionsLock.notifyAll();
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(int partition) {
        return Optional.ofNullable(partitionToConsumedOffsets.get(partition));
    }

    public boolean assignedPartition(int partition) {
        return assignedMetaPartitions.contains(partition);
    }

    private void ensureNotClosed() {
        if (close) {
            throw new IllegalStateException("This instance is already closed");
        }
    }

    public void close() {
        if (!close) {
            close = true;
            consumer.wakeup();
        }
    }
}