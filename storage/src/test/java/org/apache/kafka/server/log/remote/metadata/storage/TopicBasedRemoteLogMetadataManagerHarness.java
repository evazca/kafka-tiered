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

import kafka.api.IntegrationTestHarness;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP;

public class TopicBasedRemoteLogMetadataManagerHarness extends IntegrationTestHarness {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerHarness.class);

    protected static final int METADATA_TOPIC_PARTITIONS_COUNT = 3;
    protected static final int METADATA_TOPIC_REPLICATION_FACTOR = 2;
    protected static final long METADATA_TOPIC_RETENTION_MS = 24 * 60 * 60 * 1000L;

    private final Time time = new MockTime(1);
    private TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager;

    protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
        return Collections.emptyMap();
    }

    public void initialize(Set<TopicIdPartition> topicIdPartitions) {
        // Call setup to start the cluster.
        super.setUp();

        // Make sure the remote log metadata topic is created before it is used.
        createMetadataTopic();

        topicBasedRemoteLogMetadataManager = new TopicBasedRemoteLogMetadataManager();
        topicBasedRemoteLogMetadataManager.setTime(time);

        // Initialize TopicBasedRemoteLogMetadataManager.
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList());
        configs.put("broker.id", 0);
        configs.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, METADATA_TOPIC_PARTITIONS_COUNT);
        configs.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, METADATA_TOPIC_REPLICATION_FACTOR);
        configs.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP, METADATA_TOPIC_RETENTION_MS);

        log.debug("TopicBasedRemoteLogMetadataManager configs before adding overridden properties: {}", configs);
        // Add override properties.
        configs.putAll(overrideRemoteLogMetadataManagerProps());
        log.debug("TopicBasedRemoteLogMetadataManager configs after adding overridden properties: {}", configs);

        topicBasedRemoteLogMetadataManager.configure(configs);
        try {
            waitUntilInitialized(120_000);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(topicIdPartitions, Collections.emptySet());
    }

    // Visible for testing.
    public void waitUntilInitialized(long waitTimeMs) throws TimeoutException {
        long startMs = System.currentTimeMillis();
        while (!topicBasedRemoteLogMetadataManager.isInitialized()) {
            long currentTimeMs = System.currentTimeMillis();
            if (currentTimeMs > startMs + waitTimeMs) {
                throw new TimeoutException("Time out reached before it is initialized successfully");
            }

            Utils.sleep(1000);
        }
    }

    @Override
    public int brokerCount() {
        return 3;
    }

    protected TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return topicBasedRemoteLogMetadataManager;
    }

    private void createMetadataTopic() {
        Properties topicConfigs = new Properties();
        topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(METADATA_TOPIC_RETENTION_MS));
        TestUtils.createTopic(zkClient(), TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME, METADATA_TOPIC_PARTITIONS_COUNT,
                              METADATA_TOPIC_REPLICATION_FACTOR, servers(), topicConfigs);
    }

    public void close() throws IOException {
        Utils.closeQuietly(topicBasedRemoteLogMetadataManager, "TopicBasedRemoteLogMetadataManager");

        // Stop the servers and zookeeper.
        tearDown();
    }
}