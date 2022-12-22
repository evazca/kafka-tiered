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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maintains a mapping from ProducerIds to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 * <p>
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 * <p>
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
public class ProducerStateManager {
    private static final Logger log = LoggerFactory.getLogger(ProducerStateManager.class.getName());

    // Remove these once UnifiedLog moves to storage module.
    public static final String DELETED_FILE_SUFFIX = ".deleted";
    public static final String PRODUCER_SNAPSHOT_FILE_SUFFIX = ".snapshot";

    public static final long LATE_TRANSACTION_BUFFER_MS = 5 * 60 * 1000;

    private static final short PRODUCER_SNAPSHOT_VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String CRC_FIELD = "crc";
    private static final String PRODUCER_ID_FIELD = "producer_id";
    private static final String LAST_SEQUENCE_FIELD = "last_sequence";
    private static final String PRODUCER_EPOCH_FIELD = "epoch";
    private static final String LAST_OFFSET_FIELD = "last_offset";
    private static final String OFFSET_DELTA_FIELD = "offset_delta";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String PRODUCER_ENTRIES_FIELD = "producer_entries";
    private static final String COORDINATOR_EPOCH_FIELD = "coordinator_epoch";
    private static final String CURRENT_TXN_FIRST_OFFSET_FIELD = "current_txn_first_offset";

    private static final int VERSION_OFFSET = 0;
    private static final int CRC_OFFSET = VERSION_OFFSET + 2;
    private static final int PRODUCER_ENTRIES_OFFSET = CRC_OFFSET + 4;

    private static final Schema PRODUCER_SNAPSHOT_ENTRY_SCHEMA = new Schema(new Field(PRODUCER_ID_FIELD, Type.INT64, "The producer ID"), new Field(PRODUCER_EPOCH_FIELD, Type.INT16, "Current epoch of the producer"), new Field(LAST_SEQUENCE_FIELD, Type.INT32, "Last written sequence of the producer"), new Field(LAST_OFFSET_FIELD, Type.INT64, "Last written offset of the producer"), new Field(OFFSET_DELTA_FIELD, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"), new Field(TIMESTAMP_FIELD, Type.INT64, "Max timestamp from the last written entry"), new Field(COORDINATOR_EPOCH_FIELD, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"), new Field(CURRENT_TXN_FIRST_OFFSET_FIELD, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"));
    private static final Schema PID_SNAPSHOT_MAP_SCHEMA = new Schema(new Field(VERSION_FIELD, Type.INT16, "Version of the snapshot file"), new Field(CRC_FIELD, Type.UNSIGNED_INT32, "CRC of the snapshot data"), new Field(PRODUCER_ENTRIES_FIELD, new ArrayOf(PRODUCER_SNAPSHOT_ENTRY_SCHEMA), "The entries in the producer table"));
    private final TopicPartition topicPartition;
    private volatile File logDir;
    public final int maxTransactionTimeoutMs;
    public final ProducerStateManagerConfig producerStateManagerConfig;
    private final Time time;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;

    private final Map<Long, ProducerStateEntry> producers = new HashMap<>();
    private long lastMapOffset = 0L;
    private long lastSnapOffset = 0L;

    // Keep track of the last timestamp from the oldest transaction. This is used
    // to detect (approximately) when a transaction has been left hanging on a partition.
    // We make the field volatile so that it can be safely accessed without a lock.
    private volatile long oldestTxnLastTimestamp = -1L;

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();

    // completed transactions whose markers are at offsets above the high watermark
    private final TreeMap<Long, TxnMetadata> unreplicatedTxns = new TreeMap<>();

    public ProducerStateManager(TopicPartition topicPartition, File logDir, int maxTransactionTimeoutMs, ProducerStateManagerConfig producerStateManagerConfig, Time time) throws IOException {
        this.topicPartition = topicPartition;
        this.logDir = logDir;
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        this.producerStateManagerConfig = producerStateManagerConfig;
        this.time = time;
        snapshots = loadSnapshots();
    }

    public boolean hasLateTransaction(long currentTimeMs) {
        long lastTimestamp = oldestTxnLastTimestamp;
        return lastTimestamp > 0 && (currentTimeMs - lastTimestamp) > maxTransactionTimeoutMs + ProducerStateManager.LATE_TRANSACTION_BUFFER_MS;
    }

    public void truncateFullyAndReloadSnapshots() throws IOException {
        log.info("Reloading the producer state snapshots");
        truncateFullyAndStartAt(0L);
        snapshots = loadSnapshots();
    }


    /**
     * Load producer state snapshots by scanning the _logDir.
     */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        List<SnapshotFile> snapshotFiles = listSnapshotFiles(logDir);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            offsetToSnapshots.put(snapshotFile.offset, snapshotFile);
        }
        return offsetToSnapshots;
    }

    /**
     * Scans the log directory, gathering all producer state snapshot files. Snapshot files which do not have an offset
     * corresponding to one of the provided offsets in segmentBaseOffsets will be removed, except in the case that there
     * is a snapshot file at a higher offset than any offset in segmentBaseOffsets.
     * <p>
     * The goal here is to remove any snapshot files which do not have an associated segment file, but not to remove the
     * largest stray snapshot file which was emitted during clean shutdown.
     */
    public void removeStraySnapshots(List<Long> segmentBaseOffsets) throws IOException {
        OptionalLong maxSegmentBaseOffset = (segmentBaseOffsets.isEmpty()) ? OptionalLong.empty() : OptionalLong.of(segmentBaseOffsets.stream().max(Long::compare).get());

        HashSet<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> snapshots = loadSnapshots();
        for (SnapshotFile snapshot : snapshots.values()) {
            long key = snapshot.offset;
            if (latestStraySnapshot.isPresent()) {
                SnapshotFile prev = latestStraySnapshot.get();
                if (!baseOffsets.contains(key)) {
                    // this snapshot is now the largest stray snapshot.
                    prev.deleteIfExists();
                    snapshots.remove(prev.offset);
                    latestStraySnapshot = Optional.of(snapshot);
                }
            } else {
                if (!baseOffsets.contains(key)) {
                    latestStraySnapshot = Optional.of(snapshot);
                }
            }
        }

        // Check to see if the latestStraySnapshot is larger than the largest segment base offset, if it is not,
        // delete the largestStraySnapshot.
        if (latestStraySnapshot.isPresent() && maxSegmentBaseOffset.isPresent()) {
            long strayOffset = latestStraySnapshot.get().offset;
            long maxOffset = maxSegmentBaseOffset.getAsLong();
            if (strayOffset < maxOffset) {
                SnapshotFile removedSnapshot = snapshots.remove(strayOffset);
                if (removedSnapshot != null) {
                    removedSnapshot.deleteIfExists();
                }
            }
        }

        this.snapshots = snapshots;
    }

    /**
     * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
     * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
     * marker written at a higher offset than the current high watermark).
     */
    public Optional<LogOffsetMetadata> firstUnstableOffset() {
        Optional<LogOffsetMetadata> unreplicatedFirstOffset = Optional.of(unreplicatedTxns.firstEntry()).map(e -> e.getValue().firstOffset);
        Optional<LogOffsetMetadata> undecidedFirstOffset = Optional.of(ongoingTxns.firstEntry()).map(e -> e.getValue().firstOffset);
        Optional<LogOffsetMetadata> result;
        if (!unreplicatedFirstOffset.isPresent()) result = undecidedFirstOffset;
        else if (!undecidedFirstOffset.isPresent()) result = unreplicatedFirstOffset;
        else if (undecidedFirstOffset.get().messageOffset < unreplicatedFirstOffset.get().messageOffset)
            result = undecidedFirstOffset;
        else result = unreplicatedFirstOffset;

        return result;
    }

    /**
     * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
     * to advance to the next unstable offset.
     */
    public void onHighWatermarkUpdated(long highWatermark) {
        removeUnreplicatedTransactions(highWatermark);
    }


    /**
     * The first undecided offset is the earliest transactional message which has not yet been committed
     * or aborted. Unlike [[firstUnstableOffset]], this does not reflect the state of replication (i.e.
     * whether a completed transaction marker is beyond the high watermark).
     */
    public Optional<Long> firstUndecidedOffset() {
        return Optional.of(ongoingTxns.firstEntry()).map(e -> e.getValue().firstOffset.messageOffset);
    }


    /**
     * Returns the last offset of this map
     */
    public long mapEndOffset() {
        return lastMapOffset;
    }

    /**
     * Get a copy of the active producers
     */
    public Map<Long, ProducerStateEntry> activeProducers() {
        return Collections.unmodifiableMap(producers);
    }

    public boolean isEmpty() {
        return producers.isEmpty() && unreplicatedTxns.isEmpty();
    }

    private void loadFromSnapshot(long logStartOffset, long currentTime) throws IOException {
        while (true) {
            Optional<SnapshotFile> latestSnapshotFileOptional = latestSnapshotFile();
            if (latestSnapshotFileOptional.isPresent()) {
                SnapshotFile snapshot = latestSnapshotFileOptional.get();
                try {
                    log.info("Loading producer state from snapshot file '{}'", snapshot.file);
                    Stream<ProducerStateEntry> loadedProducers = readSnapshot(snapshot.file).filter(producerEntry -> !isProducerExpired(currentTime, producerEntry));
                    loadedProducers.forEach(this::loadProducerEntry);
                    lastSnapOffset = snapshot.offset;
                    lastMapOffset = lastSnapOffset;
                    updateOldestTxnTimestamp();
                    return;
                } catch (CorruptSnapshotException e) {
                    log.warn("Failed to load producer snapshot from '${snapshot.file}': ${e.getMessage}");
                    removeAndDeleteSnapshot(snapshot.offset);
                }
            } else {
                lastSnapOffset = logStartOffset;
                lastMapOffset = logStartOffset;
                return;

            }
        }
    }

    public void loadProducerEntry(ProducerStateEntry entry) {
        long producerId = entry.producerId;
        producers.put(producerId, entry);
        entry.currentTxnFirstOffset.ifPresent(offset -> ongoingTxns.put(offset, new TxnMetadata(producerId, offset)));
    }

    private boolean isProducerExpired(long currentTimeMs, ProducerStateEntry producerState) {
        return !producerState.currentTxnFirstOffset.isPresent() && currentTimeMs - producerState.lastTimestamp >= producerStateManagerConfig.producerIdExpirationMs();
    }

    /**
     * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
     */
    public void removeExpiredProducers(long currentTimeMs) {
        Set<Long> keys = producers.entrySet().stream().filter(entry -> isProducerExpired(currentTimeMs, entry.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        for (Long key : keys) {
            producers.remove(key);
        }
    }

    /**
     * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
     * snapshot in range (if there is one). We delete snapshot files prior to the logStartOffset but do not remove
     * producer state from the map. This means that in-memory and on-disk state can diverge, and in the case of
     * broker failover or unclean shutdown, any in-memory state not persisted in the snapshots will be lost, which
     * would lead to UNKNOWN_PRODUCER_ID errors. Note that the log end offset is assumed to be less than or equal
     * to the high watermark.
     */
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs) throws IOException {
        // remove all out of range snapshots
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }

        if (logEndOffset != mapEndOffset()) {
            producers.clear();
            ongoingTxns.clear();
            updateOldestTxnTimestamp();

            // since we assume that the offset is less than or equal to the high watermark, it is
            // safe to clear the unreplicated transactions
            unreplicatedTxns.clear();
            loadFromSnapshot(logStartOffset, currentTimeMs);
        } else {
            onLogStartOffsetIncremented(logStartOffset);
        }
    }

    public ProducerAppendInfo prepareUpdate(long producerId, AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(new ProducerStateEntry(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin);
    }


    /**
     * Update the mapping with the given append information
     */
    public void update(ProducerAppendInfo appendInfo) {
        if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Invalid producer id " + appendInfo.producerId + " passed to update " + "for partition $topicPartition");

        log.trace("Updated producer " + appendInfo.producerId + " state to" + appendInfo);
        ProducerStateEntry updatedEntry = appendInfo.toEntry();
        ProducerStateEntry producerStateEntry = producers.get(appendInfo.producerId);
        if (producerStateEntry != null) {
            producerStateEntry.update(updatedEntry);
        } else {
            producers.put(appendInfo.producerId, updatedEntry);
        }

        appendInfo.startedTransactions().forEach(txn -> ongoingTxns.put(txn.firstOffset.messageOffset, txn));

        updateOldestTxnTimestamp();
    }


    private void updateOldestTxnTimestamp() {
        Map.Entry<Long, TxnMetadata> firstEntry = ongoingTxns.firstEntry();
        if (firstEntry == null) {
            oldestTxnLastTimestamp = -1;
        } else {
            TxnMetadata oldestTxnMetadata = firstEntry.getValue();
            ProducerStateEntry entry = producers.get(oldestTxnMetadata.producerId);
            oldestTxnLastTimestamp = entry != null ? entry.lastTimestamp : -1L;
        }
    }

    public void updateMapEndOffset(long lastOffset) {
        lastMapOffset = lastOffset;
    }

    /**
     * Get the last written entry for the given producer id.
     */
    public Optional<ProducerStateEntry> lastEntry(long producerId) {
        return Optional.of(producers.get(producerId));
    }


    /**
     * Take a snapshot at the current end offset if one does not already exist.
     */
    public void takeSnapshot() throws IOException {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile = new SnapshotFile(producerSnapshotFile(logDir, lastMapOffset));
            long start = time.hiResClockMs();
            writeSnapshot(snapshotFile.file, producers);
            log.info("Wrote producer snapshot at offset " + lastMapOffset + " with ${producers.size} producer ids in ${time.hiResClockMs() - start} ms.");

            snapshots.put(snapshotFile.offset, snapshotFile);

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;
        }
    }

    private File producerSnapshotFile(File logDir, long offset) {
        return new File(logDir, filenamePrefixFromOffset(offset) + PRODUCER_SNAPSHOT_FILE_SUFFIX);
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    private String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Update the parentDir for this ProducerStateManager and all of the snapshot files which it manages.
     */
    public void updateParentDir(File parentDir) {
        logDir = parentDir;
        snapshots.forEach((k, v) -> v.updateParentDir(parentDir));
    }

    /**
     * Get the last offset (exclusive) of the latest snapshot file.
     */
    public OptionalLong latestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = latestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> OptionalLong.of(snapshotFile.offset)).orElseGet(OptionalLong::empty);
    }

    /**
     * Get the last offset (exclusive) of the oldest snapshot file.
     */
    public OptionalLong oldestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = oldestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> OptionalLong.of(snapshotFile.offset)).orElseGet(OptionalLong::empty);
    }

    /**
     * Visible for testing
     */
    public Optional<SnapshotFile> snapshotFileForOffset(long offset) {
        return Optional.of(snapshots.get(offset));
    }

    /**
     * Remove any unreplicated transactions lower than the provided logStartOffset and bring the lastMapOffset forward
     * if necessary.
     */
    public void onLogStartOffsetIncremented(long logStartOffset) {
        removeUnreplicatedTransactions(logStartOffset);

        if (lastMapOffset < logStartOffset) lastMapOffset = logStartOffset;

        lastSnapOffset = latestSnapshotOffset().orElse(logStartOffset);
    }

    private void removeUnreplicatedTransactions(long offset) {
        Iterator<Map.Entry<Long, TxnMetadata>> iterator = unreplicatedTxns.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TxnMetadata> txnEntry = iterator.next();
            OptionalLong lastOffset = txnEntry.getValue().lastOffset;
            if (lastOffset.isPresent() && lastOffset.getAsLong() < offset) iterator.remove();
        }
    }


    /**
     * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
     */
    public void truncateFullyAndStartAt(long offset) throws IOException {
        producers.clear();
        ongoingTxns.clear();
        unreplicatedTxns.clear();
        for (SnapshotFile snapshotFile : snapshots.values()) {
            removeAndDeleteSnapshot(snapshotFile.offset);
        }
        lastSnapOffset = 0L;
        lastMapOffset = offset;
        updateOldestTxnTimestamp();
    }

    /**
     * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
     * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
     * transaction index, but the completion must be done only after successfully appending to the index.
     */
    public long lastStableOffset(CompletedTxn completedTxn) {
        return findNextIncompleteTxn(completedTxn.producerId).map(x -> x.firstOffset.messageOffset).orElse(completedTxn.lastOffset + 1);
    }

    private Optional<TxnMetadata> findNextIncompleteTxn(long producerId) {
        for (TxnMetadata txnMetadata : ongoingTxns.values()) {
            if (txnMetadata.producerId != producerId) {
                return Optional.of(txnMetadata);
            }
        }
        return Optional.empty();
    }

    /**
     * Mark a transaction as completed. We will still await advancement of the high watermark before
     * advancing the first unstable offset.
     */
    public void completeTxn(CompletedTxn completedTxn) {
        TxnMetadata txnMetadata = ongoingTxns.remove(completedTxn.firstOffset);
        if (txnMetadata == null)
            throw new IllegalArgumentException("Attempted to complete transaction $completedTxn on partition $topicPartition " + "which was not started");

        txnMetadata.lastOffset = OptionalLong.of(completedTxn.lastOffset);
        unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata);
        updateOldestTxnTimestamp();
    }

    public void deleteSnapshotsBefore(long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
    }

    private Optional<SnapshotFile> oldestSnapshotFile() {
        return Optional.of(snapshots.firstEntry()).map(x -> x.getValue());
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.of(snapshots.lastEntry()).map(e -> e.getValue());
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and deletes the backing snapshot file.
     */
    private void removeAndDeleteSnapshot(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) snapshotFile.deleteIfExists();
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and renames the backing snapshot file to have the Log.DeletionSuffix.
     * <p>
     * Note: This method is safe to use with async deletes. If a race occurs and the snapshot file
     * is deleted without this ProducerStateManager instance knowing, the resulting exception on
     * SnapshotFile rename will be ignored and None will be returned.
     */
    public Optional<SnapshotFile> removeAndMarkSnapshotForDeletion(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) {
            // If the file cannot be renamed, it likely means that the file was deleted already.
            // This can happen due to the way we construct an intermediate producer state manager
            // during log recovery, and use it to issue deletions prior to creating the "real"
            // producer state manager.
            //
            // In any case, removeAndMarkSnapshotForDeletion is intended to be used for snapshot file
            // deletion, so ignoring the exception here just means that the intended operation was
            // already completed.
            try {
                snapshotFile.renameTo(DELETED_FILE_SUFFIX);
                return Optional.of(snapshotFile);
            } catch (NoSuchFileException ex) {
                log.info("Failed to rename producer state snapshot ${snapshot.file.getAbsoluteFile} with deletion suffix because it was already deleted");
            }
        }
        return Optional.empty();
    }

    public static Stream<ProducerStateEntry> readSnapshot(File file) throws IOException {
        try {
            byte[] buffer = Files.readAllBytes(file.toPath());
            Struct struct = PID_SNAPSHOT_MAP_SCHEMA.read(ByteBuffer.wrap(buffer));

            Short version = struct.getShort(VERSION_FIELD);
            if (version != PRODUCER_SNAPSHOT_VERSION)
                throw new CorruptSnapshotException("Snapshot contained an unknown file version $version");

            long crc = struct.getUnsignedInt(CRC_FIELD);
            long computedCrc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.length - PRODUCER_ENTRIES_OFFSET);
            if (crc != computedCrc)
                throw new CorruptSnapshotException("Snapshot is corrupt (CRC is no longer valid). " + "Stored crc: $crc. Computed crc: $computedCrc");

            List<ProducerStateEntry> entries = new ArrayList<>();
            for (Object producerEntryObj : struct.getArray(PRODUCER_ENTRIES_FIELD)) {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                long producerId = producerEntryStruct.getLong(PRODUCER_ID_FIELD);
                short producerEpoch = producerEntryStruct.getShort(PRODUCER_EPOCH_FIELD);
                int seq = producerEntryStruct.getInt(LAST_SEQUENCE_FIELD);
                long offset = producerEntryStruct.getLong(LAST_OFFSET_FIELD);
                long timestamp = producerEntryStruct.getLong(TIMESTAMP_FIELD);
                int offsetDelta = producerEntryStruct.getInt(OFFSET_DELTA_FIELD);
                int coordinatorEpoch = producerEntryStruct.getInt(COORDINATOR_EPOCH_FIELD);
                long currentTxnFirstOffset = producerEntryStruct.getLong(CURRENT_TXN_FIRST_OFFSET_FIELD);
                List<BatchMetadata> lastAppendedDataBatches = new ArrayList<>();
                if (offset >= 0) lastAppendedDataBatches.add(new BatchMetadata(seq, offset, offsetDelta, timestamp));

                entries.add(new ProducerStateEntry(producerId, lastAppendedDataBatches, producerEpoch, coordinatorEpoch, timestamp, (currentTxnFirstOffset >= 0) ? OptionalLong.of(currentTxnFirstOffset) : OptionalLong.empty()));
            }

            return entries.stream();
        } catch (SchemaException e) {
            throw new CorruptSnapshotException("Snapshot failed schema validation: " + e.getMessage());
        }
    }

    private static void writeSnapshot(File file, Map<Long, ProducerStateEntry> entries) throws IOException {
        Struct struct = new Struct(PID_SNAPSHOT_MAP_SCHEMA);
        struct.set(VERSION_FIELD, PRODUCER_SNAPSHOT_VERSION);
        struct.set(CRC_FIELD, 0L); // we'll fill this after writing the entries
        List<Struct> structEntries = new ArrayList<>(entries.size());
        for (Map.Entry<Long, ProducerStateEntry> producerIdEntry : entries.entrySet()) {
            Long producerId = producerIdEntry.getKey();
            ProducerStateEntry entry = producerIdEntry.getValue();
            Struct producerEntryStruct = struct.instance(PRODUCER_ENTRIES_FIELD);
            producerEntryStruct.set(PRODUCER_ID_FIELD, producerId).set(PRODUCER_EPOCH_FIELD, entry.producerEpoch).set(LAST_SEQUENCE_FIELD, entry.lastSeq()).set(LAST_OFFSET_FIELD, entry.lastDataOffset()).set(OFFSET_DELTA_FIELD, entry.lastOffsetDelta()).set(TIMESTAMP_FIELD, entry.lastTimestamp).set(COORDINATOR_EPOCH_FIELD, entry.coordinatorEpoch).set(CURRENT_TXN_FIRST_OFFSET_FIELD, entry.currentTxnFirstOffset.orElse(-1L));
            structEntries.add(producerEntryStruct);
        }
        struct.set(PRODUCER_ENTRIES_FIELD, structEntries.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        // now fill in the CRC
        long crc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.limit() - PRODUCER_ENTRIES_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            fileChannel.force(true);
        }
    }

    private static boolean isSnapshotFile(File file) {
        return file.getName().endsWith(PRODUCER_SNAPSHOT_FILE_SUFFIX);
    }

    // visible for testing
    public static List<SnapshotFile> listSnapshotFiles(File dir) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.list(dir.toPath())) {
                return paths.filter(path -> path.toFile().isFile() && isSnapshotFile(path.toFile())).map(path -> new SnapshotFile(path.toFile())).collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * The batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
     * batch with the highest sequence is at the tail of the queue. We will retain at most {@link ProducerStateEntry#NumBatchesToRetain}
     * elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
     */
    public static class ProducerStateEntry {
        public static final int NumBatchesToRetain = 5;
        public final long producerId;
        private final List<BatchMetadata> batchMetadata;
        short producerEpoch;
        int coordinatorEpoch;
        long lastTimestamp;
        OptionalLong currentTxnFirstOffset;

        public ProducerStateEntry(long producerId) {
            this(producerId, Collections.emptyList(), RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, OptionalLong.empty());
        }

        public ProducerStateEntry(long producerId, List<BatchMetadata> batchMetadata, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset) {
            this.producerId = producerId;
            this.batchMetadata = batchMetadata;
            this.producerEpoch = producerEpoch;
            this.coordinatorEpoch = coordinatorEpoch;
            this.lastTimestamp = lastTimestamp;
            this.currentTxnFirstOffset = currentTxnFirstOffset;
        }

        public int firstSeq() {
            return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.get(0).firstSeq();
        }


        public long firstDataOffset() {
            return isEmpty() ? -1L : batchMetadata.get(0).firstOffset();
        }

        public int lastSeq() {
            return isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.get(batchMetadata.size() - 1).lastSeq;
        }

        public long lastDataOffset() {
            return isEmpty() ? -1L : batchMetadata.get(batchMetadata.size() - 1).lastOffset;
        }

        public int lastOffsetDelta() {
            return isEmpty() ? 0 : batchMetadata.get(batchMetadata.size() - 1).offsetDelta;
        }

        public boolean isEmpty() {
            return batchMetadata.isEmpty();
        }

        public void addBatch(short producerEpoch, int lastSeq, long lastOffset, int offsetDelta, long timestamp) {
            maybeUpdateProducerEpoch(producerEpoch);
            addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
            this.lastTimestamp = timestamp;
        }

        public boolean maybeUpdateProducerEpoch(short producerEpoch) {
            if (this.producerEpoch != producerEpoch) {
                batchMetadata.clear();
                this.producerEpoch = producerEpoch;
                return true;
            } else {
                return false;
            }
        }

        private void addBatchMetadata(BatchMetadata batch) {
            if (batchMetadata.size() == ProducerStateEntry.NumBatchesToRetain) batchMetadata.remove(0);
            batchMetadata.add(batch);
        }

        public void update(ProducerStateEntry nextEntry) {
            maybeUpdateProducerEpoch(nextEntry.producerEpoch);
            while (!nextEntry.batchMetadata.isEmpty()) addBatchMetadata(nextEntry.batchMetadata.remove(0));
            this.coordinatorEpoch = nextEntry.coordinatorEpoch;
            this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset;
            this.lastTimestamp = nextEntry.lastTimestamp;
        }

        public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
            if (batch.producerEpoch() != producerEpoch) return Optional.empty();
            else return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
        }

        // Return the batch metadata of the cached batch having the exact sequence range, if any.
        Optional<BatchMetadata> batchWithSequenceRange(int firstSeq, int lastSeq) {
            Stream<BatchMetadata> duplicate = batchMetadata.stream().filter(metadata -> firstSeq == metadata.firstSeq() && lastSeq == metadata.lastSeq);
            return duplicate.findFirst();
        }

        public List<BatchMetadata> batchMetadata() {
            return Collections.unmodifiableList(batchMetadata);
        }

        public short producerEpoch() {
            return producerEpoch;
        }

        public int coordinatorEpoch() {
            return coordinatorEpoch;
        }

        public long lastTimestamp() {
            return lastTimestamp;
        }

        public OptionalLong currentTxnFirstOffset() {
            return currentTxnFirstOffset;
        }
    }

    public static class SnapshotFile {
        public static long offsetFromFileName(String fileName) {
            return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
        }

        private volatile File file;
        public final long offset;

        public SnapshotFile(File file) {
            this(file, offsetFromFileName(file.getName()));
        }

        public SnapshotFile(File file, long offset) {
            this.file = file;
            this.offset = offset;
        }

        public boolean deleteIfExists() throws IOException {
            boolean deleted = Files.deleteIfExists(file.toPath());
            if (deleted) {
                log.info("Deleted producer state snapshot ${file.getAbsolutePath}");
            } else {
                log.info("Failed to delete producer state snapshot ${file.getAbsolutePath} because it does not exist.");
            }
            return deleted;
        }

        public void updateParentDir(File parentDir) {
            String name = file.getName();
            file = new File(parentDir, name);
        }

        public File file() {
            return file;
        }

        public void renameTo(String newSuffix) throws IOException {
            File renamed = new File(Utils.replaceSuffix(file.getPath(), "", newSuffix));
            try {
                Utils.atomicMoveWithFallback(file.toPath(), renamed.toPath());
            } finally {
                file = renamed;
            }
        }
    }
}
