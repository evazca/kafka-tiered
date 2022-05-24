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

import java.util.Collections
import java.util.Optional
import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.log.{LeaderOffsetIncremented, Log, LogAppendInfo}
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.checkpoints.{CheckpointReadBuffer, LeaderEpochCheckpointFile}
import kafka.server.epoch.EpochEntry
import kafka.utils.Implicits._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.log.remote.storage.RemoteStorageException
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}
import scala.compat.java8.OptionConverters._

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
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_7_IV1) 12
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 11
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV2) 10
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 8
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_1_1_IV0) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 6
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_2_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0

  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  override protected val isOffsetForLeaderEpochSupported: Boolean = brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV2
  override protected val isTruncationOnFetchSupported = ApiVersion.isTruncationOnFetchSupported(brokerConfig.interBrokerProtocolVersion)
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
    val records = toMemoryRecords(partitionData.records)

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
    try {
      val clientResponse = leaderEndpoint.sendRequest(fetchRequest)
      val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse[Records]]
      if (!fetchSessionHandler.handleResponse(fetchResponse)) {
        Map.empty
      } else {
        fetchResponse.responseData.asScala
      }
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition,
                                                       currentLeaderEpoch: Int): (Int, Long) = {
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV1)
      fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    else
      fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_TIMESTAMP)
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition,
                                                     currentLeaderEpoch: Int): Long = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetsRequest.LATEST_TIMESTAMP)._2
  }

  private def fetchOffsetFromLeader(topicPartition: TopicPartition,
                                    currentLeaderEpoch: Int, earliestOrLatest: Long): (Int, Long) = {

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
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          (responsePartition.leaderEpoch, responsePartition.offset)
        else
          // There is no leader epoch before 0.10, so the returned epoch can be -1.
          (-1, responsePartition.oldStyleOffsets.get(0))
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
      val requestBuilder = FetchRequest.Builder
        .forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .toForget(fetchData.toForget)
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

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long,
                                                 logStartOffset: Option[Long] = None): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false, logStartOffset)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {

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
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

  /**
    * Returns, if it exists, the epoch at which the given offset is defined on the leader. This method relies on
    * an [[OffsetsForLeaderEpochRequest]] sent to the leader. If successful, a response with the tuple (LE, EO) is
    * received, where:
    *
    * - LE is the largest leader epoch smaller than or equal to the epoch at the leader local log start offset - 1,
    *   i.e. LE(ELO) - 1 where ELO stands for earliest local offset on the leader log.
    * - EO is the end offset of the leader epoch LE, which is the start offset of the leader epoch immediately
    *   following LE, i.e. ELO.
    *
    * We know that LE < LE(ELO) because we are requesting the end offset of the leader epoch  at LE(ELO) - 1
    * Two cases to consider:
    *
    * - EO < ELO, in which case the search offset belongs to the leader epoch LE(ELO).
    * - EO == searchOffset + 1 = ELO, in which case the search offset belongs to the leader epoch LE.
    *
    * @param partition The topic-partition targeted by the search.
    * @param searchOffset The offset which leader epoch is requested from the leader of the topic-partition.
    * @param epochForLeaderLocalLogStartOffset The epoch the local start offset is assigned to, LE(ELO).
    * @param currentLeaderEpoch The current leader epoch (on the leader) of the topic-partition.
    * @return If it is found, the leader epoch assigned to the search offset in the leader's log.
    */
  private[server] def findEpochForOffsetOnLeader(partition: TopicPartition,
                                                 searchOffset: Long,
                                                 epochForLeaderLocalLogStartOffset: Int,
                                                 currentLeaderEpoch: Int): Option[Int] = {

    if (epochForLeaderLocalLogStartOffset == 0) {
      // The leader epoch at ELO is 0, hence any offset preceding ELO will also be assigned to the leader epoch 0.
      // Return directly that leader epoch, because we want to avoid an OffsetsForLeaderEpoch request with - 1
      // as a leader epoch, which is of reserved semantic in Kafka.
      debug(s"Not fetching leader epoch for offset $searchOffset from leader as the leader epoch at ELO is 0")
      Some(0)

    } else {
      val partitionsWithEpochs = Map(partition -> new EpochData()
        .setPartition(partition.partition())
        .setCurrentLeaderEpoch(currentLeaderEpoch)
        .setLeaderEpoch(epochForLeaderLocalLogStartOffset - 1))

      val maybeEpochEndOffset = fetchEpochEndOffsets(partitionsWithEpochs).get(partition)

      maybeEpochEndOffset.flatMap { epochEndOffset =>
        if (epochEndOffset.errorCode() != Errors.NONE.code())
          None
        else if (epochEndOffset.endOffset() > searchOffset)
          // Always use the leader epoch from returned epochEndOffset.
          // This gives the respective leader epoch, that will handle any gaps in epochs.
          // For ex, leader epoch cache contains:
          // leader-epoch   start-offset
          //  0 		          20
          //  1 		          85
          //  <2> - gap no messages were appended in this leader epoch.
          //  3 		          90
          //  4 		          98
          // There is a gap in leader epoch. For leaderLocalLogStartOffset as 90, leader-epoch is 3.
          // fetchEpochEndOffsets(2) will return leader-epoch as 1, end-offset as 90.
          // So, for offset 89, we should return leader epoch as 1 like below.
          Some(epochEndOffset.leaderEpoch())
        else
          Some(epochForLeaderLocalLogStartOffset)
      }
    }
  }

  /**
    * Resolves the leader epoch map and producer snapshot of the given topic-partition from the data available in
    * the remote storage. This method is called after the leader of the partition indicated that the last fetch
    * offset used by this follower is present in the remote storage and not on the leader's local storage.
    *
    * The resolution happens by first retrieving the RemoteLogSegmentMetadata of the segment which contains
    * the offset immediately preceding the local start offset on the leader log (also called earliest local
    * offset ELO), ELO - 1. The leader epoch at ELO (LE(ELO)) has been provided to this method, however the
    * leader epoch at ELO - 1 is unknown and need to be resolved from the leader. Once available, the remote log
    * segment metadata can be found by invoking the plugin API with the offset ELO - 1 and leader epoch LE(ELO - 1).
    * The leader epoch cache file and producer snapshot file are then downloaded from the remote storage and a
    * local copy of them is kept.
    *
    * @param partition The topic-partition to reconstruct the state for.
    * @param currentLeaderEpoch The current leader epoch (on the leader) of the topic-partition.
    * @param leaderLocalLogStartOffset The local start offset of the log on the leader, ELO.
    * @param epochForLeaderLocalLogStartOffset The epoch the local start offset is assigned to, LE(ELO).
    * @param leaderLogStartOffset The start offset of the log, including the segments exclusively stored in the
    *                             remote storage.
    */
  override protected[server] def buildRemoteLogAuxState(partition: TopicPartition,
                                                        currentLeaderEpoch: Int,
                                                        leaderLocalLogStartOffset: Long,
                                                        epochForLeaderLocalLogStartOffset: Int,
                                                        leaderLogStartOffset: Long): Unit = {

    replicaMgr.localLog(partition).foreach { log =>
      if (log.rlmEnabled && log.config.remoteStorageEnable) {
        replicaMgr.remoteLogManager.foreach { rlm =>

          // Assuming the log is correctly tiered, the (leader local log start offset - 1) must be in the remote
          // storage (though it may not be the last one, since the local and remote storage can overlap).
          val tieredOffset = leaderLocalLogStartOffset - 1

          // The leader epoch assigned to the search offset on the leader needs to be found.
          // In the most general cases, it can be any epoch in the inclusive range [0, leader epoch at ELO].
          //
          // What to do if the leader epoch is not found? Can it be transient or permanent? Should the partition
          // be marked as failed? In the current implementation, the truncation will fail and the follower will
          // retry truncation forever, until this method succeeds.
          val maybeTieredEpoch = findEpochForOffsetOnLeader(
            partition, tieredOffset, epochForLeaderLocalLogStartOffset, currentLeaderEpoch)

          maybeTieredEpoch match {
            case Some(lookupEpoch) =>
              rlm.fetchRemoteLogSegmentMetadata(partition, lookupEpoch, tieredOffset).asScala match {
                case Some(rlsMetadata) =>
                  val epochStream = rlm.storageManager().fetchIndex(rlsMetadata, IndexType.LEADER_EPOCH)
                  val epochs = readLeaderEpochCheckpoint(epochStream)

                  // Truncate the existing local log before restoring the leader epoch cache and producer snapshots.
                  truncateFullyAndStartAt(partition, leaderLocalLogStartOffset, Some(leaderLogStartOffset))

                  log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)
                  epochs.foreach(epochEntry => {
                    log.leaderEpochCache.map(cache => cache.assign(epochEntry.epoch, epochEntry.startOffset))
                  })

                  info(s"Updated the epoch cache from remote tier till offset: $leaderLocalLogStartOffset " +
                    s"with size: ${epochs.size} for $partition")

                  // restore producer snapshot
                  val snapshotFile = Log.producerSnapshotFile(log.dir, leaderLocalLogStartOffset)
                  Files.copy(rlm.storageManager().fetchIndex(rlsMetadata, IndexType.PRODUCER_SNAPSHOT),
                    snapshotFile.toPath, StandardCopyOption.REPLACE_EXISTING)
                  log.producerStateManager.reloadSegments()
                  log.loadProducerState(leaderLocalLogStartOffset, reloadFromCleanShutdown = false)

                  info(s"Built the leader epoch cache and producer snapshots from remote tier for $partition. " +
                    s"Active producers: ${log.producerStateManager.activeProducers.size}, " +
                    s"LeaderLogStartOffset: $leaderLogStartOffset")

                case None =>
                  throw new RemoteStorageException(s"Could not find remote metadata for $partition at offset " +
                    s"$tieredOffset and leader epoch $lookupEpoch")
              }

            case None =>
              throw new RemoteStorageException(s"Could not find the leader epoch of $partition at offset " +
                s"$tieredOffset from its leader ${sourceBroker.id}")
          }
        }
      } else {
        truncateFullyAndStartAt(partition, leaderLocalLogStartOffset)
      }
    }
  }

  private def readLeaderEpochCheckpoint(stream: InputStream): collection.Seq[EpochEntry] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
    try {
      val readBuffer = new CheckpointReadBuffer[EpochEntry](location = "", bufferedReader, version = 0,
        LeaderEpochCheckpointFile.Formatter)
      readBuffer.read()
    } finally {
      bufferedReader.close()
    }
  }
}
