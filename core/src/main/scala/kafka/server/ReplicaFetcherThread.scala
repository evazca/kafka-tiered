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

package kafka.server

import kafka.cluster.BrokerEndPoint
import kafka.log.{LeaderOffsetIncremented, LogAppendInfo, UnifiedLog}
import kafka.server.AbstractFetcherThread.{ReplicaFetch, ResultWithPartitions}
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import kafka.utils.Implicits._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.server.common.CheckpointFile.CheckpointReadBuffer
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.log.remote.storage.{RemoteStorageException, RemoteStorageManager}

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.util.{Collections, Optional}
import scala.collection.{Map, mutable}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicaQuota,
                           leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                failedPartitions,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  private val replicaId = brokerConfig.brokerId
  private val logContext = new LogContext(s"[ReplicaFetcher replicaId=$replicaId, leaderId=${sourceBroker.id}, " +
    s"fetcherId=$fetcherId] ")
  this.logIdent = logContext.logPrefix

  private val leaderEndpoint = leaderEndpointBlockingSend.getOrElse(
    new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-$replicaId-fetcher-$fetcherId", logContext))

  // Visible for testing
  private[server] val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV1)) 14
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_3_1_IV0)) 13
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_7_IV1)) 12
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_3_IV1)) 11
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_1_IV2)) 10
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_0_IV1)) 8
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_1_1_IV0)) 7
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_11_0_IV1)) 5
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_11_0_IV0)) 4
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_10_1_IV1)) 3
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_10_0_IV0)) 2
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_9_0)) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_8_IV0)) 4
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_3_IV1)) 3
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_1_IV1)) 2
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_0_IV0)) 1
    else 0

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV1)) 8
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1)) 7
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_8_IV0)) 6
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_2_IV1)) 5
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_1_IV1)) 4
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_2_0_IV1)) 3
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_11_0_IV0)) 2
    else if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_10_1_IV2)) 1
    else 0

  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  override protected val isOffsetForLeaderEpochSupported: Boolean = brokerConfig.interBrokerProtocolVersion.isOffsetForLeaderEpochSupported
  override protected val isTruncationOnFetchSupported = brokerConfig.interBrokerProtocolVersion.isTruncationOnFetchSupported
  val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leaderEndpoint.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of leader endpoint $leaderEndpoint after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leaderEndpoint.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close leader endpoint $leaderEndpoint after shutting down replica fetcher thread", t)
    }
  }

  // process fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (logTrace)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)

    if (logTrace)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    val followerHighWatermark = log.updateHighWatermark(partitionData.highWatermark)
    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)
    if (logTrace)
      trace(s"Follower set replica high watermark for partition $topicPartition to $followerHighWatermark")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  override protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
    val clientResponse = try {
      leaderEndpoint.sendRequest(fetchRequest)
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
    val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    if (!fetchSessionHandler.handleResponse(fetchResponse, clientResponse.requestHeader().apiVersion())) {
      // If we had a session topic ID related error, throw it, otherwise return an empty fetch data map.
      if (fetchResponse.error == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
        throw Errors.forCode(fetchResponse.error().code()).exception()
      } else {
        Map.empty
      }
    } else {
      fetchResponse.responseData(fetchSessionHandler.sessionTopicNames, clientResponse.requestHeader().apiVersion()).asScala
    }
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): (Int, Long) = {
    if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV1))
      fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    else
      fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_TIMESTAMP)
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): (Int, Long) = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.LATEST_TIMESTAMP)
  }

  private def fetchOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int, earliestOrLatest: Long): (Int, Long) = {
    val topic = new ListOffsetsTopic()
      .setName(topicPartition.topic)
      .setPartitions(Collections.singletonList(
        new ListOffsetsPartition()
          .setPartitionIndex(topicPartition.partition)
          .setCurrentLeaderEpoch(currentLeaderEpoch)
          .setTimestamp(earliestOrLatest)))
    val requestBuilder = ListOffsetsRequest.Builder.forReplica(listOffsetRequestVersion, replicaId)
      .setTargetTimes(Collections.singletonList(topic))

    val clientResponse = leaderEndpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetsResponse]
    val responsePartition = response.topics.asScala.find(_.name == topicPartition.topic).get
      .partitions.asScala.find(_.partitionIndex == topicPartition.partition).get

    Errors.forCode(responsePartition.errorCode) match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion.isAtLeast(IBP_0_10_1_IV2))
          (responsePartition.leaderEpoch, responsePartition.offset )
        else
          (responsePartition.leaderEpoch, responsePartition.oldStyleOffsets.get(0))
      case error => throw error.exception
    }
  }

  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    val partitionsWithError = mutable.Set[TopicPartition]()

    val builder = fetchSessionHandler.newBuilder(partitionMap.size, false)
    partitionMap.forKeyValue { (topicPartition, fetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      if (fetchState.isReadyForFetch && !shouldFollowerThrottle(quota, fetchState, topicPartition)) {
        try {
          val logStartOffset = this.logStartOffset(topicPartition)
          val lastFetchedEpoch = if (isTruncationOnFetchSupported)
            fetchState.lastFetchedEpoch.map(_.asInstanceOf[Integer]).asJava
          else
            Optional.empty[Integer]
          builder.add(topicPartition, new FetchRequest.PartitionData(
            fetchState.topicId.getOrElse(Uuid.ZERO_UUID),
            fetchState.fetchOffset,
            logStartOffset,
            fetchSize,
            Optional.of(fetchState.currentLeaderEpoch),
            lastFetchedEpoch))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }

    val fetchData = builder.build()
    val fetchRequestOpt = if (fetchData.sessionPartitions.isEmpty && fetchData.toForget.isEmpty) {
      None
    } else {
      val version: Short = if (fetchRequestVersion >= 13 && !fetchData.canUseTopicIds) 12 else fetchRequestVersion
      val requestBuilder = FetchRequest.Builder
        .forReplica(version, replicaId, maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .removed(fetchData.toForget)
        .replaced(fetchData.toReplace)
        .metadata(fetchData.metadata)
      Some(ReplicaFetch(fetchData.sessionPartitions(), requestBuilder))
    }

    ResultWithPartitions(fetchRequestOpt, partitionsWithError)
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(tp)
    val log = partition.localLogOrException

    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    if (offsetTruncationState.offset < log.highWatermark)
      warn(s"Truncating $tp to offset ${offsetTruncationState.offset} below high watermark " +
        s"${log.highWatermark}")

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  override def fetchEpochEndOffsetsFromLeader(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {

    if (partitions.isEmpty) {
      debug("Skipping leaderEpoch request since all partitions do not have an epoch")
      return Map.empty
    }

    val topics = new OffsetForLeaderTopicCollection(partitions.size)
    partitions.forKeyValue { (topicPartition, epochData) =>
      var topic = topics.find(topicPartition.topic)
      if (topic == null) {
        topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic)
        topics.add(topic)
      }
      topic.partitions.add(epochData)
    }

    val epochRequest = OffsetsForLeaderEpochRequest.Builder.forFollower(
      offsetForLeaderEpochRequestVersion, topics, brokerConfig.brokerId)
    debug(s"Sending offset for leader epoch request $epochRequest")

    try {
      val response = leaderEndpoint.sendRequest(epochRequest)
      val responseBody = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse]
      debug(s"Received leaderEpoch response $response")
      responseBody.data.topics.asScala.flatMap { offsetForLeaderTopicResult =>
        offsetForLeaderTopicResult.partitions.asScala.map { offsetForLeaderPartitionResult =>
          val tp = new TopicPartition(offsetForLeaderTopicResult.topic, offsetForLeaderPartitionResult.partition)
          tp -> offsetForLeaderPartitionResult
        }
      }.toMap
    } catch {
      case t: Throwable =>
        warn(s"Error when sending leader epoch request for $partitions", t)

        // if we get any unexpected exception, mark all partitions with an error
        val error = Errors.forException(t)
        partitions.map { case (tp, _) =>
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(error.code)
        }
    }
  }

  /**
   * To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   * the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

  /**
   * It tries to build the required state for this partition from leader and remote storage so that it can start
   * fetching records from the leader.
   */
  override protected def buildRemoteLogAuxState(partition: TopicPartition,
                                                currentLeaderEpoch: Int,
                                                leaderLocalLogStartOffset: Long,
                                                epochForLeaderLocalLogStartOffset: Int,
                                                leaderLogStartOffset: Long): Unit = {

    def fetchEarlierEpochEndOffset(epoch:Int): EpochEndOffset = {
        val previousEpoch = epoch - 1
        // Find the end-offset for the epoch earlier to the given epoch from the leader
        val partitionsWithEpochs = Map(partition -> new EpochData().setPartition(partition.partition())
          .setCurrentLeaderEpoch(currentLeaderEpoch)
          .setLeaderEpoch(previousEpoch))
        val maybeEpochEndOffset = fetchEpochEndOffsetsFromLeader(partitionsWithEpochs).get(partition)
        if (maybeEpochEndOffset.isEmpty) {
          throw new KafkaException("No response received for partition: " + partition);
        }

        val epochEndOffset = maybeEpochEndOffset.get
        if (epochEndOffset.errorCode() != Errors.NONE.code()) {
          throw Errors.forCode(epochEndOffset.errorCode()).exception()
        }

        epochEndOffset
    }

    replicaMgr.localLog(partition).foreach { log =>
      if (log.remoteStorageSystemEnable && log.config.remoteLogConfig.remoteStorageEnable) {
        replicaMgr.remoteLogManager.foreach { rlm =>

          // Find the respective leader epoch for (leaderLocalLogStartOffset - 1). We need to build the leader epoch cache
          // until that offset
          val highestOffsetInRemoteFromLeader = leaderLocalLogStartOffset - 1
          val targetEpoch: Int = {
            // If the existing epoch is 0, no need to fetch from earlier epoch as the desired offset(leaderLogStartOffset - 1)
            // will have the same epoch.
            if (epochForLeaderLocalLogStartOffset == 0) {
              epochForLeaderLocalLogStartOffset
            } else {
              // Fetch the earlier epoch/end-offset(exclusive) from the leader.
              val earlierEpochEndOffset = fetchEarlierEpochEndOffset(epochForLeaderLocalLogStartOffset)
              // Check if the target offset lies with in the range of earlier epoch. Here, epoch's end-offset is exclusive.
              if (earlierEpochEndOffset.endOffset > highestOffsetInRemoteFromLeader) {
                // Always use the leader epoch from returned earlierEpochEndOffset.
                // This gives the respective leader epoch, that will handle any gaps in epochs.
                // For ex, leader epoch cache contains:
                // leader-epoch   start-offset
                //  0 		          20
                //  1 		          85
                //  <2> - gap no messages were appended in this leader epoch.
                //  3 		          90
                //  4 		          98
                // There is a gap in leader epoch. For leaderLocalLogStartOffset as 90, leader-epoch is 3.
                // fetchEarlierEpochEndOffset(3) will return leader-epoch as 1, end-offset as 90.
                // So, for offset 89, we should return leader epoch as 1 like below.
                earlierEpochEndOffset.leaderEpoch()
              } else epochForLeaderLocalLogStartOffset
            }
          }

          val rlsMetadata = rlm.fetchRemoteLogSegmentMetadata(partition, targetEpoch, highestOffsetInRemoteFromLeader)

          if (rlsMetadata.isPresent) {
            val epochStream = rlm.storageManager().fetchIndex(rlsMetadata.get(), RemoteStorageManager.IndexType.LEADER_EPOCH)
            val epochs = readLeaderEpochCheckpoint(epochStream)

            // Truncate the existing local log before restoring the leader epoch cache and producer snapshots.
            truncateFullyAndStartAt(partition, leaderLocalLogStartOffset)

            log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)
            log.leaderEpochCache.foreach { cache =>
              epochs.foreach(epochEntry =>
                // Do not flush to file for each entry.
                cache.assign(epochEntry.epoch, epochEntry.startOffset, flushToFile = false)
              )
              cache.flush()
            }

            info(s"Updated the epoch cache from remote tier till offset: $leaderLocalLogStartOffset " +
              s"with size: ${epochs.size} for $partition")

            // Restore producer snapshot
            val snapshotFile = UnifiedLog.producerSnapshotFile(log.dir, leaderLocalLogStartOffset)
            val tmpSnapshotFile = new File(snapshotFile.getAbsolutePath + ".tmp");
            // Copy it to snapshot file in atomic manner.
            Files.copy(rlm.storageManager().fetchIndex(rlsMetadata.get(), RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
              tmpSnapshotFile.toPath, StandardCopyOption.REPLACE_EXISTING)
            Utils.atomicMoveWithFallback(tmpSnapshotFile.toPath, snapshotFile.toPath, false)

            // Reload producer snapshots.
            log.producerStateManager.reloadSnapshots()
            log.loadProducerState(leaderLocalLogStartOffset, reloadFromCleanShutdown = false)
            info(s"Built the leader epoch cache and producer snapshots from remote tier for $partition. " +
              s"Active producers: ${log.producerStateManager.activeProducers.size}, LeaderLogStartOffset: $leaderLogStartOffset")
          } else {
            throw new RemoteStorageException(s"Couldn't build the state from remote store for partition: $partition, " +
              s"currentLeaderEpoch: $currentLeaderEpoch, leaderLocalLogStartOffset: $leaderLocalLogStartOffset, " +
              s"leaderLogStartOffset: $leaderLogStartOffset, epoch: $targetEpoch as the previous remote log segment " +
              s"metadata was not found")
          }
        }
      } else {
        // Truncate the existing local log  and start from leader's localLogStartOffset.
        truncateFullyAndStartAt(partition, leaderLocalLogStartOffset)
      }
    }
  }

  private def readLeaderEpochCheckpoint(stream: InputStream): collection.Seq[EpochEntry] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
    try {
      val readBuffer = new CheckpointReadBuffer[EpochEntry]("", bufferedReader,  0, LeaderEpochCheckpointFile.Formatter)
      readBuffer.read().asScala.toSeq
    } finally {
      bufferedReader.close()
    }
  }

}
