/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import kafka.cluster.Partition
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.common._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.utils.{ChildFirstClassLoader, Utils}
import org.apache.kafka.server.log.remote.metadata.storage.{ClassLoaderAwareRemoteLogMetadataManager, TopicBasedRemoteLogMetadataManagerConfig}
import org.apache.kafka.server.log.remote.storage.{RemoteLogManagerConfig, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteStorageManager}

import java.io.{Closeable, InputStream}
import java.security.{AccessController, PrivilegedAction}
import java.util
import java.util.Optional
import scala.collection.{Set, mutable}
import scala.jdk.CollectionConverters._

class RemoteLogManager(rlmConfig: RemoteLogManagerConfig,
                       brokerId: Int,
                       logDir: String) extends Logging with Closeable with KafkaMetricsGroup {

  // topic ids received on leadership changes
  private val topicIds: mutable.Map[String, Uuid] = mutable.Map.empty

  private val remoteLogStorageManager: ClassLoaderAwareRemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteLogStorageManager, logDir = logDir)

  private var closed = false

  private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = {
    AccessController.doPrivileged(new PrivilegedAction[ClassLoaderAwareRemoteStorageManager] {
      private val classPath = rlmConfig.remoteStorageManagerClassPath()

      override def run(): ClassLoaderAwareRemoteStorageManager = {
        val classLoader =
          if (classPath != null && classPath.trim.nonEmpty) {
            new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          } else {
            this.getClass.getClassLoader
          }
        val delegate = classLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
          .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
        new ClassLoaderAwareRemoteStorageManager(delegate, classLoader)
      }
    })
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    remoteLogStorageManager.configure(rsmProps)
  }

  private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    AccessController.doPrivileged(new PrivilegedAction[RemoteLogMetadataManager] {
      private val classPath = rlmConfig.remoteLogMetadataManagerClassPath

      override def run(): RemoteLogMetadataManager = {
        var classLoader = this.getClass.getClassLoader
        if (classPath != null && classPath.trim.nonEmpty) {
          classLoader = new ChildFirstClassLoader(classPath, classLoader)
          val delegate = classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[RemoteLogMetadataManager]
          new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader)
        } else {
          classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[RemoteLogMetadataManager]
        }
      }
    })
  }

  private def configureRLMM(): Unit = {
    val rlmmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteLogMetadataManagerProps().asScala.foreach { case (k, v) => rlmmProps.put(k, v) }
    rlmmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    rlmmProps.put(KafkaConfig.LogDirProp, logDir)
    remoteLogMetadataManager.configure(rlmmProps)
  }

  def onEndpointCreated(): Unit = {
    // Initialize and configure RSM and RLMM
    configureRSM()
    configureRLMM()
  }

  def storageManager(): RemoteStorageManager = {
    remoteLogStorageManager
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
   * @param partitionsBecomeFollower partitions that have become followers on this broker.
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition],
                         partitionsBecomeFollower: Set[Partition],
                         topicIds: util.Map[String, Uuid]): Unit = {
    debug(s"Received leadership changes for leaders: $partitionsBecomeLeader and followers: $partitionsBecomeFollower")
    topicIds.forEach((topic, uuid) => this.topicIds.put(topic, uuid))

    // Partitions logs are available when this callback is invoked.
    // Compact topics and internal topics are filtered here as they are not supported with tiered storage.
    def filterPartitions(partitions: Set[Partition]): Set[Partition] = {
      partitions.filterNot(partition => Topic.isInternal(partition.topic) ||
        partition.log.exists(log => log.config.compact || !log.config.remoteLogConfig.remoteStorageEnable) ||
        partition.topicPartition.topic().equals(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME)
      )
    }

    val followerTopicPartitions = filterPartitions(partitionsBecomeFollower).map(partition =>
      new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))

    val filteredLeaderPartitions = filterPartitions(partitionsBecomeLeader)
    val leaderTopicPartitions = filteredLeaderPartitions.map(partition =>
      new TopicIdPartition(topicIds.get(partition.topic), partition.topicPartition))

    debug(s"Effective topic partitions after filtering compact and internal topics, leaders: $leaderTopicPartitions " +
      s"and followers: $followerTopicPartitions")

    if (leaderTopicPartitions.nonEmpty || followerTopicPartitions.nonEmpty) {
      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderTopicPartitions.asJava, followerTopicPartitions.asJava)
    }

  }


  /**
   * Stops partitions for copying segments, building indexes and deletes the partition in remote storage if delete flag
   * is set as true.
   *
   * @param topicPartition topic partition to be stopped.
   * @param delete         flag to indicate whether the given topic partitions to be deleted or not.
   */
  def stopPartitions(topicPartition: TopicPartition, delete: Boolean): Unit = {
    // Unassign topic partitions from RLM leader/follower
    val topicIdPartition =
      topicIds.remove(topicPartition.topic()) match {
        case Some(uuid) => Some(new TopicIdPartition(uuid, topicPartition))
        case None => None
      }
    debug(s"Removed partition: $topicIdPartition from topic-ids")
  }

  def fetchRemoteLogSegmentMetadata(tp: TopicPartition,
                                    epochForOffset: Int,
                                    offset: Long): Optional[RemoteLogSegmentMetadata] = {
    val topicIdPartition =
      topicIds.get(tp.topic()) match {
        case Some(uuid) => Some(new TopicIdPartition(uuid, tp))
        case None => None
      }

    if (topicIdPartition.isEmpty) {
      throw new KafkaException("No topic id registered for topic partition: " + tp)
    }
    remoteLogMetadataManager.remoteLogSegmentMetadata(topicIdPartition.get, epochForOffset, offset)
  }

  def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata, startPos)
      val remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream)
      var batch: RecordBatch = null

      def nextBatch(): RecordBatch = {
        batch = remoteLogInputStream.nextBatch()
        batch
      }

      while (nextBatch() != null) {
        if (batch.maxTimestamp >= timestamp && batch.lastOffset >= startingOffset) {
          batch.iterator.asScala.foreach(record => {
            if (record.timestamp >= timestamp && record.offset >= startingOffset)
              return Some(new TimestampAndOffset(record.timestamp, record.offset, maybeLeaderEpoch(batch.partitionLeaderEpoch)))
          })
        }
      }
      None
    } finally {
      Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream")
    }
  }

  private def maybeLeaderEpoch(leaderEpoch: Int): Optional[Integer] = {
    if (leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
      Optional.empty()
    else
      Optional.of(leaderEpoch)
  }

  /**
   * Search the message offset in the remote storage based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If there is no messages in the remote storage, return None
   * - If all the messages in the remote storage have smaller offsets, return None
   * - If all the messages in the remote storage have smaller timestamps, return None
   * - If all the messages in the remote storage have larger timestamps, or no message in the remote storage has a timestamp
   * the returned offset will be max(the earliest offset in the remote storage, startingOffset) and the timestamp will
   * be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * @param timestamp      The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(tp: TopicPartition, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    //todo-tier Here also, we do not need to go through all the remote log segments to find the segments
    // containing the timestamp. We should find the  epoch for the startingOffset and then  traverse  through those
    // offsets and subsequent leader epochs to find the target timestamp/offset.
    topicIds.get(tp.topic()) match {
      case Some(uuid) =>
        val topicIdPartition = new TopicIdPartition(uuid, tp)
        remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition).asScala.foreach(rlsMetadata =>
          if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
            val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
            if (timestampOffset.isDefined)
              return timestampOffset
          }
        )
        None
      case None => None
    }
  }

  /**
   * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
   */
  def close(): Unit = {
    this synchronized {
      if (!closed) {
        Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager")
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        closed = true
      }
    }
  }

}