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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;

/**
 * This class is responsible for publishing messages into the remote log metadata topic partitions.
 */
public class ProducerManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerManager.class);

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaProducer<byte[], byte[]> producer;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;

    private volatile boolean close = false;

    public ProducerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner) {
        this.rlmmConfig = rlmmConfig;
        this.producer = new KafkaProducer<>(rlmmConfig.producerProperties());
        topicPartitioner = rlmmTopicPartitioner;
    }

    public RecordMetadata publishMessage(TopicIdPartition topicIdPartition,
                                         RemoteLogMetadata remoteLogMetadata) throws KafkaException {
        ensureNotClosed();

        int metadataPartitionNo = topicPartitioner.metadataPartition(topicIdPartition);
        log.debug("Publishing metadata message of partition:[{}] into metadata topic partition:[{}] with payload: [{}]",
                topicIdPartition, metadataPartitionNo, remoteLogMetadata);

        ProducerCallback callback = new ProducerCallback();
        try {
            if (metadataPartitionNo >= rlmmConfig.metadataTopicPartitionsCount()) {
                // This should never occur as long as metadata partitions always remain the same.
                throw new KafkaException("Chosen partition no " + metadataPartitionNo +
                                         " is more than the partition count: " + rlmmConfig.metadataTopicPartitionsCount());
            }
            producer.send(new ProducerRecord<>(rlmmConfig.remoteLogMetadataTopicName(), metadataPartitionNo, null,
                    serde.serialize(remoteLogMetadata)), callback).get();
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Exception occurred while publishing message for topicIdPartition: " + topicIdPartition, e);
        }

        if (callback.exception() != null) {
            Exception ex = callback.exception();
            if (ex instanceof KafkaException) {
                throw (KafkaException) ex;
            } else {
                throw new KafkaException(ex);
            }
        } else {
            return callback.recordMetadata();
        }
    }

    private void ensureNotClosed() {
        if (close) {
            throw new IllegalStateException("This instance is already set in close state.");
        }
    }

    public void close() {
        close = true;

        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                log.error("Error encountered while closing the producer", e);
            }
        }
    }

    private static class ProducerCallback implements Callback {
        private volatile RecordMetadata recordMetadata;
        private volatile Exception exception;

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;
            this.exception = exception;
        }

        public RecordMetadata recordMetadata() {
            return recordMetadata;
        }

        public Exception exception() {
            return exception;
        }
    }

}