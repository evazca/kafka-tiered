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

import kafka.server.epoch.LeaderEpochFileCache
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
import org.apache.kafka.common.requests.{AbstractRequest, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Mock of a broker endpoint which serves only [[OffsetsForLeaderEpochRequest]] based on predefined leader
  * epoch cache files. This mock does not involve any actual RPC.
  *
  * The partitions for which a leader epoch cache is defined are assumed to be online on the leader.
  * */
class OffsetsForLeaderEpochServer extends BlockingSend {
  private val leaderEpochFiles: mutable.Map[TopicPartition, LeaderEpochFileCache] = mutable.Map()
  private val currentLeaderEpochs: mutable.Map[TopicPartition, Int] = mutable.Map()

  /**
    * Add the given leader epoch cache for the topic-partition.
    * Set the value of the current leader epoch for this partition on the leader.
    */
  def add(topicPartition: TopicPartition,
          leaderEpochFileCache: LeaderEpochFileCache,
          currentLeaderEpoch: Int): OffsetsForLeaderEpochServer = {

    leaderEpochFiles.put(topicPartition, leaderEpochFileCache)
    currentLeaderEpochs.put(topicPartition, currentLeaderEpoch)
    this
  }

  override def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = {
    val request = requestBuilder.build();

    // Only serve OffsetsForLeaderEpochRequest
    if (! (request.isInstanceOf[OffsetsForLeaderEpochRequest])) {
      throw new UnsupportedOperationException("This mock only serves OffsetsForLeaderEpochRequest")
    }

    val offsetsForLeaderEpochRequest = request.asInstanceOf[OffsetsForLeaderEpochRequest]

    val epochEndOffsetsPerTopic: mutable.Map[String, mutable.Buffer[EpochEndOffset]] = mutable.Map()

    offsetsForLeaderEpochRequest.data().topics().asScala
      .flatMap(topic => {
        epochEndOffsetsPerTopic.put(topic.topic(), mutable.Buffer[EpochEndOffset]())
        Seq(topic.topic()).zip(topic.partitions().asScala)
      })
      .foreach(_ match { case (topic, partition) => {
        val topicPartition = new TopicPartition(topic, partition.partition())

        val epochEndOffset = {
          val currentLeaderEpoch = currentLeaderEpochs(topicPartition)
          val leaderEpochCache = leaderEpochFiles(topicPartition)

          if (currentLeaderEpoch > partition.currentLeaderEpoch()) {
            new EpochEndOffset()
              .setPartition(partition.partition())
              .setErrorCode(Errors.FENCED_LEADER_EPOCH.code)

          } else if (currentLeaderEpoch < partition.currentLeaderEpoch()) {
            new EpochEndOffset()
              .setPartition(partition.partition())
              .setErrorCode(Errors.UNKNOWN_LEADER_EPOCH.code)

          } else {
            val (foundEpoch, foundOffset) = leaderEpochCache.endOffsetFor(partition.leaderEpoch())

            val result = new EpochEndOffset()
              .setPartition(partition.partition())
              .setErrorCode(Errors.NONE.code)

            if (foundOffset != UNDEFINED_EPOCH_OFFSET) {
              result
                .setLeaderEpoch(foundEpoch)
                .setEndOffset(foundOffset)
            }

            result
          }
        }

        epochEndOffsetsPerTopic(topic) += epochEndOffset
      }
    })

    val result = epochEndOffsetsPerTopic.map((_: (String, mutable.Buffer[EpochEndOffset])) match {
      case (topic, epochEndOffsets) =>
        new OffsetForLeaderTopicResult()
          .setTopic(topic)
          .setPartitions(epochEndOffsets.toList.asJava)
    })

    val endOffsetsForAllTopics = new OffsetForLeaderTopicResultCollection(result.asJava.iterator())
    val response =
      new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData().setTopics(endOffsetsForAllTopics))

    new ClientResponse(null, null, null, 0, 0, false, null, null, response)
  }

  override def initiateClose(): Unit = {}

  override def close(): Unit = {}
}
