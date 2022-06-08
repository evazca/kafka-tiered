/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tiered.storage

import org.apache.kafka.common.config.TopicConfig

/**
 * Test Case:
 *
 *    Given a cluster of brokers {B0, B1} and a topic-partition Ta-p0.
 *    The purpose of this test is to exercise multiple failure scenarios on the cluster upon
 *    on a single-broker outage and loss of the first-tiered storage on one broker, that is:
 *
 *    - No segments were uploaded to the remote storage for Ta-p0 and the log-start-offset doesn't starts with zero;
 *    - Loss of the active log segment on B0;
 *    - Loss of availability of broker B0;
 *
 *    Acceptance:
 *    -----------
 *    - Follower B0 fetch requests should be handled by the leader B1 when the given fetch offset is out-of-range.
 *    - B0 restores the availability both active and remote log segments if any upon restart.
 */
class FollowerFetchRequestWithOutOfRangeFetchOffsetTest extends TieredStorageTestHarness {
  private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    builder

      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, enableRemoteLogStorage = false)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, batchSize = 1)
      .deleteRecords(topicA, p0, beforeOffset = 2)
      // enable remote log storage
      .updateTopicConfig(topicA, Map(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG -> "true"), Seq.empty)

       // Stop B0 and consume the records from the leader replica B1.
      .expectLeader(topicA, p0, broker0)
      .stop(broker0)
      .expectLeader(topicA, p0, broker1)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 1, expectedRecordsFromSecondTier = 0)

      /*
       * Restore previous leader with an empty storage. The active segment is expected to be
       * replicated from the new leader.
       * Note that a preferred leader election is manually triggered for broker 0 to avoid
       * waiting on the election which would be automatically triggered.
       */
      .eraseBrokerStorage(broker0)
      .start(broker0)
      .expectLeader(topicA, p0, broker0, electLeader = true)
      // consume the records from the leader replica B0 and confirm that the remote fetch request count is zero.
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 1, expectedRecordsFromSecondTier = 0)
      .expectFetchFromTieredStorage(broker0, topicA, p0, remoteFetchRequestCount = 0)
  }
}
