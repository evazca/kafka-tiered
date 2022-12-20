package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.internals.LazyIndex;
import org.apache.kafka.server.log.internals.OffsetIndex;
import org.apache.kafka.server.log.internals.OffsetPosition;
import org.apache.kafka.server.log.internals.TimeIndex;
import org.apache.kafka.server.log.internals.TimestampOffset;
import org.apache.kafka.server.log.internals.TransactionIndex;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.utils.ServerUtils;
import org.apache.kafka.utils.ShutdownableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * This is a LRU cache of remote index files stored in `$logdir/remote-log-index-cache`. This is helpful to avoid
 * re-fetching the index files like offset, time indexes from the remote storage for every fetch call.
 */
public class RemoteIndexCache implements Closeable {

    private final Logger log = LoggerFactory.getLogger(RemoteIndexCache.class);

    private static final String DirName = "remote-log-index-cache";

    // All the below suffixes will be replaced with UnifiedLog once it is moved to storage module.
    private static final String TmpFileSuffix = ".tmp";
    private static final String DeletedFileSuffix = ".deleted";
    private static final String IndexFileSuffix = ".index";
    private static final String TimeIndexFileSuffix = ".timeindex";
    private static final String TxnIndexFileSuffix = ".txnindex";

    private final File cacheDir;
    private volatile boolean closed = false;

    private final LinkedBlockingQueue<Entry> expiredIndexes = new LinkedBlockingQueue<>();
    private final Object lock = new Object();
    private final RemoteStorageManager remoteStorageManager;
    private final Map<Uuid, Entry> entries;

    private final ShutdownableThread cleanerThread;

    public RemoteIndexCache(RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this(1024, remoteStorageManager, logDir);
    }

    /**
     * Created RemoteIndexCache with the given configs.
     *
     * @param maxSize              maximum number of segment index entries to be cached.
     * @param remoteStorageManager RemoteStorageManager instance, to be used in fetching indexes.
     * @param logDir               log directory
     */
    public RemoteIndexCache(int maxSize, RemoteStorageManager remoteStorageManager, String logDir) throws IOException {
        this.remoteStorageManager = remoteStorageManager;
        cacheDir = new File(logDir, DirName);

        entries = new LinkedHashMap<Uuid, RemoteIndexCache.Entry>(maxSize,0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Uuid, RemoteIndexCache.Entry> eldest) {
                if (this.size() > maxSize) {
                    RemoteIndexCache.Entry entry = eldest.getValue();
                    // Mark the entries for cleanup, background thread will clean them later.
                    try {
                        entry.markForCleanup();
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                    expiredIndexes.add(entry);
                    return true;
                } else {
                    return false;
                }
            }
        };

        init();

        // Start cleaner thread that will clean the expired entries.
        cleanerThread = createCLeanerThread();
        cleanerThread.start();
    }

    private ShutdownableThread createCLeanerThread() {
        ShutdownableThread thread = new ShutdownableThread("remote-log-index-cleaner") {
            public void doWork() {
                while (!closed) {
                    try {
                        Entry entry = expiredIndexes.take();
                        log.info("Cleaning up index entry $entry");
                        entry.cleanup();
                    } catch (InterruptedException ex) {
                        log.info("Cleaner thread was interrupted", ex);
                    } catch (Exception ex) {
                        log.error("Error occurred while fetching/cleaning up expired entry", ex);
                    }
                }
            }
        };
        thread.setDaemon(true);

        return thread;
    }

    private void init() throws IOException {
        if (cacheDir.mkdir())
            log.info("Created Cache dir [{}] successfully", cacheDir);

        // Delete any .deleted files remained from the earlier run of the broker.
        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            paths.forEach(path -> {
                if (path.endsWith(DeletedFileSuffix)) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
            });
        }

        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            paths.forEach(path -> {

                String pathStr = path.getFileName().toString();
                String name = pathStr.substring(0, pathStr.lastIndexOf("_") + 1);

                // Create entries for each path if all the index files exist.
                int firstIndex = name.indexOf('_');
                int offset = Integer.parseInt(name.substring(0, firstIndex));
                Uuid uuid = Uuid.fromString(name.substring(firstIndex + 1, name.lastIndexOf('_')));

                if (!entries.containsKey(uuid)) {
                    File offsetIndexFile = new File(cacheDir, name + IndexFileSuffix);
                    File timestampIndexFile = new File(cacheDir, name + TimeIndexFileSuffix);
                    File txnIndexFile = new File(cacheDir, name + TxnIndexFileSuffix);

                    try {
                        if (offsetIndexFile.exists() && timestampIndexFile.exists() && txnIndexFile.exists()) {

                            LazyIndex<OffsetIndex> offsetIndex = LazyIndex.forOffset(offsetIndexFile, offset, Integer.MAX_VALUE, false);
                            offsetIndex.get().sanityCheck();

                            LazyIndex<TimeIndex> timeIndex = LazyIndex.forTime(timestampIndexFile, offset, Integer.MAX_VALUE, false);
                            timeIndex.get().sanityCheck();

                            TransactionIndex txnIndex = new TransactionIndex(offset, txnIndexFile);
                            txnIndex.sanityCheck();

                            Entry entry = new Entry(offsetIndex, timeIndex, txnIndex);
                            entries.put(uuid, entry);
                        } else {
                            // Delete all of them if any one of those indexes is not available for a specific segment id
                            Files.deleteIfExists(offsetIndexFile.toPath());
                            Files.deleteIfExists(timestampIndexFile.toPath());
                            Files.deleteIfExists(txnIndexFile.toPath());
                        }
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
            });
        }
    }

    private <T> T loadIndexFile(String fileName, String suffix, RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                Function<RemoteLogSegmentMetadata, InputStream> fetchRemoteIndex,
                                Function<File, T> readIndex) throws IOException {
        File indexFile = new File(cacheDir, fileName + suffix);
        T index = null;
        if (indexFile.exists()) {
            try {
                index = readIndex.apply(indexFile);
            } catch (CorruptRecordException ex) {
                log.info("Error occurred while loading the stored index", ex);
            }
        }

        if (index == null) {
            File tmpIndexFile = new File(indexFile.getParentFile(), indexFile.getName() + RemoteIndexCache.TmpFileSuffix);

            try (InputStream inputStream = fetchRemoteIndex.apply(remoteLogSegmentMetadata);) {
                Files.copy(inputStream, tmpIndexFile.toPath());
            }

            Utils.atomicMoveWithFallback(tmpIndexFile.toPath(), indexFile.toPath(), false);
            index = readIndex.apply(indexFile);
        }

        return index;
    }

    public Entry getIndexEntry(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        if (closed)
            throw new IllegalStateException("Instance is already closed.");

        synchronized (lock) {
            return entries.computeIfAbsent(remoteLogSegmentMetadata.remoteLogSegmentId().id(), (Uuid uuid) -> {
                long startOffset = remoteLogSegmentMetadata.startOffset();
                // uuid.toString uses URL encoding which is safe for filenames and URLs.
                String fileName = startOffset + "_" + uuid.toString() + "_";

                try {
                    LazyIndex<OffsetIndex> offsetIndex = loadIndexFile(fileName, IndexFileSuffix, remoteLogSegmentMetadata,
                            rlsMetadata -> {
                                try {
                                    return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.OFFSET);
                                } catch (RemoteStorageException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            file -> {
                                try {
                                    LazyIndex<OffsetIndex> index = LazyIndex.forOffset(file, startOffset, Integer.MAX_VALUE, false);
                                    index.get().sanityCheck();
                                    return index;
                                } catch (IOException e) {
                                    throw new KafkaException(e);
                                }
                            });

                    LazyIndex<TimeIndex> timeIndex = loadIndexFile(fileName, TimeIndexFileSuffix, remoteLogSegmentMetadata,
                            rlsMetadata -> {
                                try {
                                    return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TIMESTAMP);
                                } catch (RemoteStorageException e) {
                                    throw new KafkaException(e);
                                }
                            },
                            file -> {
                                LazyIndex<TimeIndex> index = LazyIndex.forTime(file, startOffset, Integer.MAX_VALUE, false);
                                try {
                                    index.get().sanityCheck();
                                } catch (IOException e) {
                                    throw new KafkaException(e);
                                }
                                return index;
                            });

                    TransactionIndex txnIndex = loadIndexFile(fileName, TxnIndexFileSuffix, remoteLogSegmentMetadata,
                            rlsMetadata -> {
                                try {
                                    return remoteStorageManager.fetchIndex(rlsMetadata, IndexType.TRANSACTION);
                                } catch (RemoteStorageException e) {
                                    throw new KafkaException(e);
                                }
                            },
                            file -> {
                                TransactionIndex index = null;
                                try {
                                    index = new TransactionIndex(startOffset, file);
                                } catch (IOException e) {
                                    throw new KafkaException(e);
                                }
                                index.sanityCheck();
                                return index;
                            });

                    return new Entry(offsetIndex, timeIndex, txnIndex);
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            });
        }
    }

    public int lookupOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) throws IOException {
        return getIndexEntry(remoteLogSegmentMetadata).lookupOffset(offset).position;
    }

    public int lookupTimestamp(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long timestamp, long startingOffset) throws IOException {
        return getIndexEntry(remoteLogSegmentMetadata).lookupTimestamp(timestamp, startingOffset).position;
    }

    public void close() {
        closed = true;
        try {
            cleanerThread.shutdown();
        } catch (InterruptedException e) {
            // ignore interrupted exception
        }

        // Close all the opened indexes.
        synchronized (lock) {
            entries.values().forEach(Entry::close);
        }
    }

    public Map<Uuid, Entry> entries() {
        return Collections.unmodifiableMap(entries);
    }

    private static class Entry {

        public final LazyIndex<OffsetIndex> offsetIndex;
        public final LazyIndex<TimeIndex> timeIndex;
        public final TransactionIndex txnIndex;

        public Entry(LazyIndex<OffsetIndex> offsetIndex, LazyIndex<TimeIndex> timeIndex, TransactionIndex txnIndex) {
            this.offsetIndex = offsetIndex;
            this.timeIndex = timeIndex;
            this.txnIndex = txnIndex;
        }

        private boolean markedForCleanup = false;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        public OffsetPosition lookupOffset(long targetOffset) throws IOException {
            lock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");
                else return offsetIndex.get().lookup(targetOffset);
            } finally {
                lock.readLock().unlock();
            }
        }

        public OffsetPosition lookupTimestamp(long timestamp, long startingOffset) throws IOException {
            lock.readLock().lock();
            try {
                if (markedForCleanup) throw new IllegalStateException("This entry is marked for cleanup");

                TimestampOffset timestampOffset = timeIndex.get().lookup(timestamp);
                return offsetIndex.get().lookup(Math.max(startingOffset, timestampOffset.offset));
            } finally {
                lock.readLock().unlock();
            }
        }

        public void markForCleanup() throws IOException {
            lock.writeLock().lock();
            try {
                if (!markedForCleanup) {
                    markedForCleanup = true;

                    offsetIndex.renameTo(new File(ServerUtils.replaceSuffix(offsetIndex.file().getPath(), "", DeletedFileSuffix)));
                    txnIndex.renameTo(new File(ServerUtils.replaceSuffix(txnIndex.file().getPath(), "", DeletedFileSuffix)));
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void cleanup() throws IOException {
            markForCleanup();

            try {
                ServerUtils.tryAll(Arrays.asList(() -> {
                    offsetIndex.deleteIfExists();
                    return null;
                }, () -> {
                    timeIndex.deleteIfExists();
                    return null;
                }, () -> {
                    txnIndex.deleteIfExists();
                    return null;
                }));
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        public void close() {
            Arrays.asList(offsetIndex, timeIndex).forEach(index -> {
                try {
                    index.close();
                } catch (Exception e) {
                    // ignore exception.
                }
            });

            Utils.closeQuietly(txnIndex, "Closing the transaction index.");
        }
    }
}
