/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.stream.Stream

import kafka.cluster.BrokerEndPoint
import kafka.server.FindOffsetForEpochOnLeaderTest.{newReplicaFetcherThread, topicPartition}
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.SystemTime
import org.easymock.EasyMock.mock
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

object FindOffsetForEpochOnLeaderTest {
  private val topicPartition = new TopicPartition("Barton", 0)

  val leaderEpochCache = {
    val leaderEpochCache = newLeaderEpochCache(10)
    leaderEpochCache.assign(epoch = 0, startOffset = 0)
    leaderEpochCache.assign(epoch = 2, startOffset = 5)
    leaderEpochCache.assign(epoch = 10, startOffset = 7)
    leaderEpochCache
  }

  def testCases(): Stream[Arguments] = {
    Seq(
      // Log empty on leader and follower.
      Arguments.of(newLeaderEpochCache(0),
        0.asInstanceOf[AnyRef],
        0.asInstanceOf[AnyRef],
        0.asInstanceOf[AnyRef],
        0.asInstanceOf[AnyRef],
        Some(0)),

      Arguments.of(leaderEpochCache,
        0.asInstanceOf[AnyRef], // search offset.
        0.asInstanceOf[AnyRef], // epoch for leader local start offset (1) is 0.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(0) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        1.asInstanceOf[AnyRef], // search offset.
        0.asInstanceOf[AnyRef], // epoch for leader local start offset (2) is 0.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(0) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        2.asInstanceOf[AnyRef], // search offset.
        0.asInstanceOf[AnyRef], // epoch for leader local start offset (3) is 0.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(0) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        3.asInstanceOf[AnyRef], // search offset.
        0.asInstanceOf[AnyRef], // epoch for leader local start offset (4) is 0.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(0) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        4.asInstanceOf[AnyRef], // search offset.
        2.asInstanceOf[AnyRef], // epoch for leader local start offset (5) is 2.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(0) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        5.asInstanceOf[AnyRef], // search offset.
        2.asInstanceOf[AnyRef], // epoch for leader local start offset (6) is 2.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(2) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        6.asInstanceOf[AnyRef], // search offset.
        2.asInstanceOf[AnyRef], // epoch for leader local start offset (7) is 3.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(2) // expected leader epoch.
      ),

      Arguments.of(leaderEpochCache,
        7.asInstanceOf[AnyRef], // search offset.
        10.asInstanceOf[AnyRef], // epoch for leader local start offset (8) is 10.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        10.asInstanceOf[AnyRef], // current leader epoch on the follower.
        Some(10) // expected leader epoch.
      ),

      // Fencing
      Arguments.of(leaderEpochCache,
        7.asInstanceOf[AnyRef], // search offset.
        3.asInstanceOf[AnyRef], // epoch for leader local start offset (8) is 3.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        11.asInstanceOf[AnyRef], // current leader epoch on the follower.
        None // expected leader epoch.
      ),

      // Fencing
      Arguments.of(leaderEpochCache,
        7.asInstanceOf[AnyRef], // search offset.
        3.asInstanceOf[AnyRef], // epoch for leader local start offset (8) is 3.
        10.asInstanceOf[AnyRef], // current leader epoch on the leader.
        9.asInstanceOf[AnyRef], // current leader epoch on the follower.
        None // expected leader epoch.
      )
    ).asJava.stream()
  }

  private def newLeaderEpochCache(logEndOffset: Long): LeaderEpochFileCache = {
    val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
      private var epochs: Seq[EpochEntry] = Seq()
      override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
      override def read(): Seq[EpochEntry] = this.epochs
    }

    new LeaderEpochFileCache(topicPartition, () => logEndOffset, checkpoint)
  }

  def newReplicaFetcherThread(blockingSend: BlockingSend): ReplicaFetcherThread = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)

    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    new ReplicaFetcherThread(
      name = "Piledriver",
      fetcherId = 0,
      sourceBroker = new BrokerEndPoint(0, "localhost", 1000),
      brokerConfig = config,
      failedPartitions = new FailedPartitions,
      replicaMgr = replicaManager,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(blockingSend))
  }

}

class FindOffsetForEpochOnLeaderTest {
  @ParameterizedTest
  @MethodSource(Array("testCases"))
  def test(leaderEpochOnLeader: LeaderEpochFileCache,
           searchOffset: Int,
           epochForLeaderLocalLogStartOffset: Int,
           currentLeaderEpochOnLeader: Int,
           currentLeaderEpochInRequest: Int,
           expectedLeaderEpoch: Option[Int]): Unit = {

    val leaderEndpoint = new OffsetsForLeaderEpochServer()
      .add(topicPartition, leaderEpochOnLeader, currentLeaderEpochOnLeader)

    val replicaFetcher = newReplicaFetcherThread(leaderEndpoint)

    val maybeLeaderEpoch = replicaFetcher.findEpochForOffsetOnLeader(
       topicPartition, searchOffset, epochForLeaderLocalLogStartOffset, currentLeaderEpochInRequest)

    assertEquals(expectedLeaderEpoch, maybeLeaderEpoch)
  }
}
