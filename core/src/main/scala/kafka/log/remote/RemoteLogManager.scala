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
import kafka.log.UnifiedLog
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.InMemoryLeaderEpochCheckpoint
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{RecordBatch, RemoteLogInputStream}
import org.apache.kafka.common.utils.{ChildFirstClassLoader, Time, Utils}
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager
import org.apache.kafka.server.log.remote.storage.{RemoteLogManagerConfig, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteStorageManager}

import java.io._
import java.security.{AccessController, PrivilegedAction}
import java.util
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Future, TimeUnit}
import scala.collection.Set
import scala.jdk.CollectionConverters._

/**
 * This class is responsible for
 *  - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances.
 *  - receives any leader and follower replica events and partition stop events and act on them
 *  - also provides APIs to fetch indexes, metadata about remote log segments.
 *
 * @param rlmConfig
 * @param brokerId
 * @param logDir
 */
class RemoteLogManager(brokerId: Int,
                       rlmConfig: RemoteLogManagerConfig,
                       fetchLog: TopicPartition => Option[UnifiedLog],
                       updateRemoteLogStartOffset: (TopicPartition, Long) => Unit,
                       brokerTopicStats: BrokerTopicStats,
                       time: Time = Time.SYSTEM,
                       logDir: String) extends Logging with Closeable with KafkaMetricsGroup {

  // topic ids received on leadership changes
  private val topicPartitionIds: ConcurrentMap[TopicPartition, Uuid] = new ConcurrentHashMap[TopicPartition, Uuid]()

  private val remoteStorageManager: RemoteStorageManager = createRemoteStorageManager()
  private val remoteLogMetadataManager: RemoteLogMetadataManager = createRemoteLogMetadataManager()

  private val indexCache = new RemoteIndexCache(remoteStorageManager = remoteStorageManager, logDir = logDir)

  private var closed = false
  private val leaderOrFollowerTasks: ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture] =
    new ConcurrentHashMap[TopicIdPartition, RLMTaskWithFuture]()

  private val delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs
  private val poolSize = rlmConfig.remoteLogManagerThreadPoolSize
  private val rlmScheduledThreadPool = new RLMScheduledThreadPool(poolSize)

  private[remote] def rlmScheduledThreadPoolSize: Int = rlmScheduledThreadPool.poolSize()

  private[remote] def leaderOrFollowerTasksSize: Int = leaderOrFollowerTasks.size()

  private def doHandleLeaderOrFollowerPartitions(topicPartition: TopicIdPartition,
                                                 convertToLeaderOrFollower: RLMTask => Unit): Unit = {
    val rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition, (tp: TopicIdPartition) => {
      val task = new RLMTask(brokerId, tp, fetchLog, updateRemoteLogStartOffset, time, brokerTopicStats, remoteStorageManager, remoteLogMetadataManager)
      // Set this upfront when it is getting initialized instead of doing it after scheduling.
      convertToLeaderOrFollower(task)
      info(s"Created a new task: $task and getting scheduled")
      val future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS)
      RLMTaskWithFuture(task, future)
    })
    convertToLeaderOrFollower(rlmTaskWithFuture.rlmTask)
  }

  private[remote] def createRemoteStorageManager(): RemoteStorageManager = {
    def createDelegate(classLoader: ClassLoader): RemoteStorageManager = {
      classLoader.loadClass(rlmConfig.remoteStorageManagerClassName())
        .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteStorageManager] {
      private val classPath = rlmConfig.remoteStorageManagerClassPath()

      override def run(): RemoteStorageManager = {
        if (classPath != null && classPath.trim.nonEmpty) {
          val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          val delegate = createDelegate(classLoader)
          new ClassLoaderAwareRemoteStorageManager(delegate, classLoader)
        } else {
          createDelegate(this.getClass.getClassLoader)
        }
      }
    })
  }

  private def configureRSM(): Unit = {
    val rsmProps = new util.HashMap[String, Any]()
    rlmConfig.remoteStorageManagerProps().asScala.foreach { case (k, v) => rsmProps.put(k, v) }
    rsmProps.put(KafkaConfig.BrokerIdProp, brokerId)
    remoteStorageManager.configure(rsmProps)
  }

  private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = {
    def createDelegate(classLoader: ClassLoader) = {
      classLoader.loadClass(rlmConfig.remoteLogMetadataManagerClassName())
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[RemoteLogMetadataManager]
    }

    AccessController.doPrivileged(new PrivilegedAction[RemoteLogMetadataManager] {
      private val classPath = rlmConfig.remoteLogMetadataManagerClassPath

      override def run(): RemoteLogMetadataManager = {
        if (classPath != null && classPath.trim.nonEmpty) {
          val classLoader = new ChildFirstClassLoader(classPath, this.getClass.getClassLoader)
          val delegate = createDelegate(classLoader)
          new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader)
        } else {
          createDelegate(this.getClass.getClassLoader)
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

  def startup(): Unit = {
    // Initialize and configure RSM and RLMM. This will start RSM, RLMM resources which may need to start resources
    // in connecting to the brokers or remote storages.
    configureRSM()
    configureRLMM()
  }

  def storageManager(): RemoteStorageManager = {
    remoteStorageManager
  }

  /**
   * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
   * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
   * task to respective target state(leader or follower).
   *
   * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
   * @param partitionsBecomeFollower partitions that have become followers on this broker.
   * @param topicIds                 topic name to topic id mappings.
   */
  def onLeadershipChange(partitionsBecomeLeader: Set[Partition],
                         partitionsBecomeFollower: Set[Partition],
                         topicIds: util.Map[String, Uuid]): Unit = {
    trace(s"Received leadership changes for partitionsBecomeLeader: $partitionsBecomeLeader " +
      s"and partitionsBecomeLeader: $partitionsBecomeLeader")

    def remoteLogEnabled(partition: Partition): Boolean = {
      partition.log.exists(log => log.remoteLogEnabled())
    }

    val leaderPartitionWithEpochs = partitionsBecomeLeader.filterNot(remoteLogEnabled)
      .map(partition => {
        val topicId = topicIds.get(partition.topic)
        topicPartitionIds.put(partition.topicPartition, topicId)
        new TopicIdPartition(topicId, partition.topicPartition) -> partition.getLeaderEpoch
      }).toMap

    val followerPartitions = partitionsBecomeFollower.filterNot(remoteLogEnabled)
      .map(partition => {
        val topicId = topicIds.get(partition.topic)
        topicPartitionIds.put(partition.topicPartition, topicId)
        new TopicIdPartition(topicId, partition.topicPartition)
      })

    if (leaderPartitionWithEpochs.nonEmpty || followerPartitions.nonEmpty) {
      debug(s"Effective topic partitions after filtering compact and internal topics, " +
        s"leaders: ${leaderPartitionWithEpochs.keySet} and followers: $followerPartitions")
      remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitionWithEpochs.keySet.asJava, followerPartitions.asJava)

      followerPartitions.foreach {
        topicIdPartition => doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToFollower())
      }

      leaderPartitionWithEpochs.foreach {
        case (topicIdPartition, leaderEpoch) =>
          doHandleLeaderOrFollowerPartitions(topicIdPartition, _.convertToLeader(leaderEpoch))
      }
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
    if (delete) {
      // Delete from internal datastructures only if it is to be deleted.
      val topicIdPartition = topicPartitionIds.remove(topicPartition)
      debug(s"Removed partition: $topicIdPartition from topicPartitionIds")
    }
  }

  def fetchRemoteLogSegmentMetadata(topicPartition: TopicPartition,
                                    epochForOffset: Int,
                                    offset: Long): Optional[RemoteLogSegmentMetadata] = {
    val topicId = topicPartitionIds.get(topicPartition)

    if (topicId == null) {
      throw new KafkaException("No topic id registered for topic partition: " + topicPartition)
    }

    remoteLogMetadataManager.remoteLogSegmentMetadata(new TopicIdPartition(topicId, topicPartition), epochForOffset, offset)
  }

  private def lookupTimestamp(rlsMetadata: RemoteLogSegmentMetadata, timestamp: Long, startingOffset: Long): Option[TimestampAndOffset] = {
    val startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset)

    var remoteSegInputStream: InputStream = null
    try {
      // Search forward for the position of the last offset that is greater than or equal to the target offset
      remoteSegInputStream = remoteStorageManager.fetchLogSegment(rlsMetadata, startPos)
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
  def findOffsetByTimestamp(tp: TopicPartition,
                            timestamp: Long,
                            startingOffset: Long,
                            leaderEpochCache: LeaderEpochFileCache): Option[TimestampAndOffset] = {
    val topicId = topicPartitionIds.get(tp)
    if (topicId == null) {
      throw new KafkaException("Topic id does not exist for topic partition: " + tp)
    }

    // Get the respective epoch in which the starting offset exists.
    var maybeEpoch = leaderEpochCache.epochForOffset(startingOffset)
    while (maybeEpoch.nonEmpty) {
      remoteLogMetadataManager.listRemoteLogSegments(new TopicIdPartition(topicId, tp), maybeEpoch.get).asScala
        .foreach(rlsMetadata =>
          if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
            val timestampOffset = lookupTimestamp(rlsMetadata, timestamp, startingOffset)
            if (timestampOffset.isDefined)
              return timestampOffset
          }
        )

      // Move to the next epoch if not found with the current epoch.
      maybeEpoch = leaderEpochCache.findNextEpoch(maybeEpoch.get)
    }
    None
  }

  /**
   * Returns the leader epoch checkpoint by truncating with the given start(exclusive) and end(inclusive) offset
   *
   * @param leaderEpochCache leader-epoch checkpoint cache.
   * @param startOffset      The start offset of the checkpoint file (exclusive in the truncation).
   *                         If start offset is 6, then it will retain an entry at offset 6.
   * @param endOffset        The end offset of the checkpoint file (inclusive in the truncation)
   *                         If end offset is 100, then it will remove the entries greater than or equal to 100.
   * @return the truncated leader epoch checkpoint
   */
  private[remote] def getLeaderEpochCheckpoint(leaderEpochCache: Option[LeaderEpochFileCache], startOffset: Long, endOffset: Long): InMemoryLeaderEpochCheckpoint = {
    val checkpoint = new InMemoryLeaderEpochCheckpoint()
    leaderEpochCache
      .map(cache => cache.writeTo(checkpoint))
      .foreach { x =>
        if (startOffset >= 0) {
          x.truncateFromStart(startOffset)
        }
        x.truncateFromEnd(endOffset)
      }
    checkpoint
  }

  /**
   * Stops all the threads and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
   */
  def close(): Unit = {
    if (closed)
      warn("Trying to close an already closed RemoteLogManager")
    else this synchronized {
      // Write lock is not taken when closing this class. As, the read lock is held by other threads which might be
      // waiting on the producer future (or) trying to consume the metadata record for strong consistency.
      if (!closed) {
        // During segment copy, the RLM task publishes an event and tries to consume the same for strong consistency.
        // The active RLM task might be waiting on the producer future (or) trying to consume the record.
        // So, tasks should be cancelled first, close the RLMM, RSM, then shutdown the thread pool to close the active
        // tasks.
        leaderOrFollowerTasks.values().forEach(_.cancel())
        Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager")
        Utils.closeQuietly(remoteStorageManager, "RemoteLogMetadataManager")
        rlmScheduledThreadPool.shutdown()
        leaderOrFollowerTasks.clear()
        closed = true
      }
    }
  }

  case class RLMTaskWithFuture(rlmTask: RLMTask, future: Future[_]) {

    def cancel(): Unit = {
      rlmTask.cancel()
      try {
        future.cancel(true)
      } catch {
        case ex: Exception => error(s"Error occurred while canceling the task: $rlmTask", ex)
      }
    }
  }

}