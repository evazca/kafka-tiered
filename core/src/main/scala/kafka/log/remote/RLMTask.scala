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

import kafka.log.UnifiedLog
import kafka.server.BrokerTopicStats
import kafka.server.checkpoints.InMemoryLeaderEpochCheckpoint
import kafka.server.epoch.EpochEntry
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage._

import java.nio.ByteBuffer
import java.util.Optional
import java.{lang, util}
import scala.jdk.CollectionConverters._

class RLMTask(brokerId: Int,
              topicIdPartition: TopicIdPartition,
              fetchLog: TopicPartition => Option[UnifiedLog],
              updateRemoteLogStartOffset: (TopicPartition, Long) => Unit,
              time: Time,
              brokerTopicStats: BrokerTopicStats,
              remoteStorageManager: RemoteStorageManager,
              remoteLogMetadataManager: RemoteLogMetadataManager) extends CancellableRunnable with Logging {
  this.logIdent = s"[RLMTask:$topicIdPartition] "
  @volatile private var leaderEpoch: Int = -1

  private def isLeader(): Boolean = leaderEpoch >= 0

  // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
  // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
  // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
  // epoch cache then it starts copying the segments from the earliest epoch entry’s offset.
  private var readOffset: Long = -1

  //todo-updating log with remote index highest offset -- should this be required?
  // fetchLog(tp.topicPartition()).foreach { log => log.updateRemoteIndexHighestOffset(readOffset) }

  def convertToLeader(leaderEpochVal: Int): Unit = {
    if (leaderEpochVal < 0) {
      throw new KafkaException(s"leaderEpoch value for topic partition $topicIdPartition can not be negative")
    }
    if (this.leaderEpoch != leaderEpochVal) {
      leaderEpoch = leaderEpochVal
      info(s"Find the highest remote offset for partition: $topicIdPartition after becoming leader, leaderEpoch: $leaderEpoch")
      readOffset = findHighestRemoteOffset(topicIdPartition).get()
    }
  }

  def findHighestRemoteOffset(topicIdPartition: TopicIdPartition): Optional[lang.Long] = {
    var offset: Optional[lang.Long] = Optional.empty()
    fetchLog(topicIdPartition.topicPartition()).foreach { log =>
      log.leaderEpochCache.foreach(cache => {
        var epoch = cache.latestEpoch
        while (!offset.isPresent && epoch.isDefined) {
          offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.get)
          epoch = cache.findPreviousEpoch(epoch.get)
        }
      })
    }
    if (!offset.isPresent) {
      offset = Optional.of(-1L)
    }
    offset
  }

  def convertToFollower(): Unit = {
    leaderEpoch = -1
  }

  def copyLogSegmentsToRemote(): Unit = {
    if (isCancelled()) {
      return
    }

    try {
      fetchLog(topicIdPartition.topicPartition()).foreach { log =>

        // LSO indicates the offset below are ready to be consumed(high-watermark or committed)
        val lso = log.lastStableOffset
        if (lso < 0) {
          warn(s"lastStableOffset for partition $topicIdPartition is $lso, which should not be negative.")
        } else if (lso > 0 && readOffset < lso) {
          // Copy segments only till the min of high-watermark or stable-offset
          // Remote storage should contain only committed/acked messages
          val fetchOffset = lso
          debug(s"Checking for segments to copy, readOffset: $readOffset and fetchOffset: $fetchOffset")
          val activeSegBaseOffset = log.activeSegment.baseOffset
          val sortedSegments = log.logSegments(readOffset + 1, fetchOffset).toList.sortBy(_.baseOffset)
          sortedSegments.foreach { segment =>
            // Return immediately if this task is cancelled or this is not-a-leader or current segment is active.
            if (isCancelled() || !isLeader() || segment.baseOffset >= activeSegBaseOffset) {
              info(s"Skipping copying log segments as the current task state is changed, cancelled: " +
                s"${isCancelled()} leader:${isLeader()} segment's base offset: ${segment.baseOffset} and active segment offset: ${activeSegBaseOffset}")
              return
            }

            val logFile = segment.log.file()
            val fileName = logFile.getName
            info(s"Copying $fileName to remote storage.")
            val id = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid())

            val nextOffset = segment.readNextOffset
            //todo-tier double check on this
            val endOffset = nextOffset - 1
            val producerIdSnapshotFile = log.producerStateManager.fetchSnapshot(nextOffset).orNull

            def createLeaderEpochs(): ByteBuffer = {
              val inMemoryLeaderEpochCheckpoint = new InMemoryLeaderEpochCheckpoint()
              log.leaderEpochCache
                .map(cache => cache.writeTo(inMemoryLeaderEpochCheckpoint))
                .foreach(x => {
                  x.truncateFromEnd(nextOffset)
                })

              inMemoryLeaderEpochCheckpoint.readAsByteBuffer()
            }

            def createLeaderEpochEntries(startOffset: Long): Option[collection.Seq[EpochEntry]] = {
              val maybeLeaderEpochCheckpoint = {
                val inMemoryLeaderEpochCheckpoint = new InMemoryLeaderEpochCheckpoint()
                log.leaderEpochCache
                  .map(cache => cache.writeTo(inMemoryLeaderEpochCheckpoint))
                  .map(x => {
                    x.truncateFromStart(startOffset)
                    x.truncateFromEnd(nextOffset)
                    inMemoryLeaderEpochCheckpoint
                  })
              }
              maybeLeaderEpochCheckpoint.map(x => x.read())
            }

            val leaderEpochs = createLeaderEpochs()
            val segmentLeaderEpochEntries = createLeaderEpochEntries(segment.baseOffset)
            val segmentLeaderEpochs: util.HashMap[Integer, java.lang.Long] = new util.HashMap()
            if (segmentLeaderEpochEntries.isDefined) {
              segmentLeaderEpochEntries.get.foreach(entry => segmentLeaderEpochs.put(entry.epoch, entry.startOffset))
            } else {
              val epoch = log.leaderEpochCache.flatMap(x => x.latestEntry.map(y => y.epoch)).getOrElse(0)
              segmentLeaderEpochs.put(epoch, segment.baseOffset)
            }

            val remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(id, segment.baseOffset, endOffset,
              segment.largestTimestamp, brokerId, time.milliseconds(), segment.log.sizeInBytes(),
              segmentLeaderEpochs)

            remoteLogMetadataManager.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata)

            val segmentData = new LogSegmentData(logFile.toPath, segment.lazyOffsetIndex.get.path,
              segment.lazyTimeIndex.get.path, Optional.ofNullable(segment.txnIndex.path),
              producerIdSnapshotFile.toPath, leaderEpochs)
            remoteStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, segmentData)

            val rlsmAfterCreate = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
              RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId)

            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(rlsmAfterCreate)
            brokerTopicStats.topicStats(topicIdPartition.topicPartition().topic())
              .remoteBytesOutRate.mark(remoteLogSegmentMetadata.segmentSizeInBytes())
            readOffset = endOffset
            //todo-tier-storage
            log.updateRemoteIndexHighestOffset(readOffset)
            info(s"Copied $fileName to remote storage with segment-id: ${rlsmAfterCreate.remoteLogSegmentId()}")
          }
        } else {
          debug(s"Skipping copying segments, current read offset:$readOffset is and LSO:$lso ")
        }
      }
    } catch {
      case ex: Exception =>
        brokerTopicStats.topicStats(topicIdPartition.topicPartition().topic()).failedRemoteWriteRequestRate.mark()
        if (!isCancelled()) {
          error(s"Error occurred while copying log segments of partition: $topicIdPartition", ex)
        }
    }
  }

  def handleExpiredRemoteLogSegments(): Unit = {
    if (isCancelled())
      return

    def handleLogStartOffsetUpdate(topicPartition: TopicPartition, remoteLogStartOffset: Long): Unit = {
      debug(s"Updating $topicPartition with remoteLogStartOffset: $remoteLogStartOffset")
      updateRemoteLogStartOffset(topicPartition, remoteLogStartOffset)
    }

    try {
      // cleanup remote log segments and update the log start offset if applicable.
      // Compute total size, this can be pushed to RLMM by introducing a new method instead of going through
      // the collection every time.
      val segmentMetadataList = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition).asScala.toSeq
      if (segmentMetadataList.nonEmpty) {
        fetchLog(topicIdPartition.topicPartition()).foreach { log =>
          val retentionMs = log.config.retentionMs
          val totalSize = log.size + segmentMetadataList.map(_.segmentSizeInBytes()).sum
          var (checkTimestampRetention, cleanupTs) = (retentionMs > -1, time.milliseconds() - retentionMs)
          var remainingSize = totalSize - log.config.retentionSize
          var checkSizeRetention = log.config.retentionSize > -1

          def deleteRetentionTimeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
            val isSegmentDeleted = deleteRemoteLogSegment(metadata, _.maxTimestampMs() <= cleanupTs)
            if (isSegmentDeleted && remainingSize > 0) {
              remainingSize -= metadata.segmentSizeInBytes()
            }
            isSegmentDeleted
          }

          def deleteRetentionSizeBreachedSegments(metadata: RemoteLogSegmentMetadata): Boolean = {
            deleteRemoteLogSegment(metadata, metadata =>
              // Assumption that segments contain size > 0
              if (remainingSize > 0) {
                remainingSize -= metadata.segmentSizeInBytes()
                remainingSize >= 0
              } else false
            )
          }

          // Get earliest leader epoch and start deleting the segments.
          var logStartOffset: Option[Long] = None
          log.leaderEpochCache.foreach { cache =>
            cache.epochEntries.find { epochEntry =>
              // segmentsIterator returns the segments in the ascending order.
              val segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epochEntry.epoch)
              // Continue checking for one of the time or size retentions are valid for next segment if available.
              while ((checkTimestampRetention || checkSizeRetention) && segmentsIterator.hasNext) {
                val metadata = segmentsIterator.next()
                var isSegmentDeleted = false
                if (checkTimestampRetention) {
                  if (deleteRetentionTimeBreachedSegments(metadata)) {
                    // It is fine to have logStartOffset as metadata.endOffset() + 1 as the segment offset intervals
                    // are ascending with in an epoch
                    logStartOffset = Some(metadata.endOffset() + 1)
                    info(s"Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention time " +
                      s"${retentionMs}ms breach based on the largest record timestamp in the segment")
                    isSegmentDeleted = true
                  } else {
                    // If we have any segment that is having the timestamp not eligible for deletion then
                    // we will skip all the subsequent segments for the time retention checks.
                    checkTimestampRetention = false
                  }
                }
                if (checkSizeRetention && !isSegmentDeleted) {
                  if (deleteRetentionSizeBreachedSegments(metadata)) {
                    logStartOffset = Some(metadata.endOffset() + 1)
                    info(s"Deleted remote log segment ${metadata.remoteLogSegmentId()} due to retention size " +
                      s"${log.config.retentionSize} breach. Log size after deletion will be ${remainingSize + log.config.retentionSize}.")
                  } else {
                    // If we have exhausted of segments eligible for retention size, we will skip the subsequent
                    // segments.
                    checkSizeRetention = false
                  }
                }
              }
              // Return only when both the retention checks are exhausted.
              checkTimestampRetention && checkSizeRetention
            }
          }
          logStartOffset.foreach(handleLogStartOffsetUpdate(topicIdPartition.topicPartition(), _))
        }
      }
    } catch {
      case ex: Exception =>
        if (!isCancelled())
          error(s"Error while cleaning up log segments for partition: $topicIdPartition", ex)
    }
  }

  private def deleteRemoteLogSegment(segmentMetadata: RemoteLogSegmentMetadata, predicate: RemoteLogSegmentMetadata => Boolean): Boolean = {
    if (predicate(segmentMetadata)) {
      // Publish delete segment started event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId))

      // Delete the segment in remote storage.
      remoteStorageManager.deleteLogSegmentData(segmentMetadata)

      // Publish delete segment finished event.
      remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
        new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
          RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId))
      true
    } else false
  }

  override def run(): Unit = {
    if (isCancelled())
      return
    try {
      if (isLeader()) {
        // a. copy log segments to remote store
        copyLogSegmentsToRemote()
        // b. cleanup/delete expired remote segments
        // Followers will cleanup the local log cleanup based on the local logStartOffset.
        // We do not need any cleanup on followers from remote segments perspective.
        handleExpiredRemoteLogSegments()
      } else {
        val offset = findHighestRemoteOffset(topicIdPartition)
        if (offset.isPresent) {
          fetchLog(topicIdPartition.topicPartition()).foreach { log =>
            log.updateRemoteIndexHighestOffset(offset.get())
          }
        }
      }
    } catch {
      case ex: InterruptedException =>
        if (!isCancelled())
          warn(s"Current thread for topic-partition $topicIdPartition is interrupted, this should not be rescheduled ", ex)
      case ex: Exception =>
        if (!isCancelled())
          warn(s"Current task for topic-partition $topicIdPartition received error but it will be scheduled for next iteration: ", ex)
    }
  }

  override def toString: String = {
    this.getClass.toString + s"[$topicIdPartition]"
  }
}