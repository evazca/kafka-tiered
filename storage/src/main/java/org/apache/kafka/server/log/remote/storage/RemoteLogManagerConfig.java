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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public final class RemoteLogManagerConfig {

    public static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage.manager.impl.";
    public static final String REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX = "remote.log.metadata.manager.impl.";

    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP = "remote.log.storage.system.enable";
    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC = "Whether to enable tier storage functionality in a broker or not. Valid values " +
            "are `true` or `false` and the default value is false. When it is true broker starts all the services required for tiered storage functionality.";
    public static final boolean DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE = false;

    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP = "remote.log.storage.manager.class.name";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteLogStorageManager` implementation.";

    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP = "remote.log.storage.manager.class.path";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteLogStorageManager` implementation." +
            "If specified, the RemoteLogStorageManager implementation and its dependent libraries will be loaded by a dedicated" +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same" +
            "with the standard Java class path string.";

    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP = "remote.log.metadata.manager.class.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteLogMetadataManager` implementation.";
    //todo add the default topic based RLMM class name.
    public static final String DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME = "";

    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP = "remote.log.metadata.manager.class.path";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteLogMetadataManager` implementation." +
            "If specified, the RemoteLogMetadataManager implementation and its dependent libraries will be loaded by a dedicated" +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same" +
            "with the standard Java class path string.";

    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP = "remote.log.metadata.manager.listener.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC = "Listener name of the local broker to which it should get connected if " +
            "needed by RemoteLogMetadataManager implementation.";

    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP = "remote.log.index.file.cache.total.size.bytes";
    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC = "";
    public static final long DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES = 1024 * 1024 * 1024L;

    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP = "remote.log.manager.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC = "Remote log thread pool size, which is used in scheduling tasks to copy " +
            "segments, fetch remote log indexes and clean up remote log segments.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE = 10;

    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP = "remote.log.manager.task.interval.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC = "Interval at which remote log manager runs the scheduled tasks like copy " +
            "segments, fetch remote log indexes and clean up remote log segments.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS = 30 * 1000L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP = "remote.log.manager.task.retry.backoff.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC = "";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS = 30 * 1000L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP = "remote.log.manager.task.retry.backoff.max.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC = "";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS = 30 * 1000L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS_PROP = "remote.log.manager.task.retry.jitter";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS_DOC = "";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS = 30 * 1000L;

    public static final String REMOTE_LOG_READER_THREADS_PROP = "remote.log.reader.threads";
    public static final String REMOTE_LOG_READER_THREADS_DOC = "Remote log reader thread pool size.";
    public static final int DEFAULT_REMOTE_LOG_READER_THREADS = 5;

    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP = "remote.log.reader.max.pending.tasks";
    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC = "Maximum remote log reader thread pool task queue size. If the task queue " +
            "is full, broker will stop reading remote log segments.";
    public static final int DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS = 100;

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    static {
        CONFIG_DEF.define(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, BOOLEAN,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, STRING, null, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP, STRING, null, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, STRING, null, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP, STRING, null, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, STRING, null, MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, LONG,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES, atLeast(1), LOW,
                          RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP, INT,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE, atLeast(1), MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, LONG,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS, atLeast(1), LOW,
                          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP, LONG,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS, atLeast(1), LOW,
                          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP, LONG,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS, atLeast(1), LOW,
                          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS_PROP, LONG,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS, atLeast(1), LOW,
                          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, INT, RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_READER_THREADS,
                          atLeast(1), MEDIUM, RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_DOC)
                  .define(RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP, INT,
                          RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS, atLeast(1), MEDIUM,
                          RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC);
    }

    private final boolean enableRemoteStorageSystem;
    private final String remoteStorageManagerClassName;
    private final String remoteStorageManagerClassPath;
    private final String remoteLogMetadataManagerClassName;
    private final String remoteLogMetadataManagerClassPath;
    private final long remoteLogIndexFileCacheTotalSizeBytes;
    private final int remoteLogManagerThreadPoolSize;
    private final long remoteLogManagerTaskIntervalMs;
    private final long remoteLogManagerTaskRetryBackoffMs;
    private final long remoteLogManagerTaskRetryBackoffMaxMs;
    private final long remoteLogManagerTaskRetryJitterMs;
    private final int remoteLogReaderThreads;
    private final int remoteLogReaderMaxPendingTasks;
    private final HashMap<String, Object> remoteStorageManagerProps;
    private final HashMap<String, Object> remoteLogMetadataManagerProps;
    private final String remoteLogMetadataManagerListenerName;

    public RemoteLogManagerConfig(AbstractConfig config) {
        this(config.getBoolean(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP),
             config.getString(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP),
             config.getString(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP),
             config.getLong(REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP),
             config.getInt(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP),
             config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_MS_PROP),
             config.getInt(REMOTE_LOG_READER_THREADS_PROP),
             config.getInt(REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP),
             config.originalsWithPrefix(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX),
             config.originalsWithPrefix(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX));
    }

    // Visible for testing
    public RemoteLogManagerConfig(boolean enableRemoteStorageSystem,
                                  String remoteStorageManagerClassName,
                                  String remoteStorageManagerClassPath,
                                  String remoteLogMetadataManagerClassName,
                                  String remoteLogMetadataManagerClassPath,
                                  String remoteLogMetadataManagerListenerName,
                                  long remoteLogIndexFileCacheTotalSizeBytes,
                                  int remoteLogManagerThreadPoolSize,
                                  long remoteLogManagerTaskIntervalMs,
                                  long remoteLogManagerTaskRetryBackoffMs,
                                  long remoteLogManagerTaskRetryBackoffMaxMs,
                                  long remoteLogManagerTaskRetryJitterMs,
                                  int remoteLogReaderThreads,
                                  int remoteLogReaderMaxPendingTasks,
                                  Map<String, Object> remoteStorageManagerProps,
                                  Map<String, Object> remoteLogMetadataManagerProps) {
        this.enableRemoteStorageSystem = enableRemoteStorageSystem;
        this.remoteStorageManagerClassName = remoteStorageManagerClassName;
        this.remoteStorageManagerClassPath = remoteStorageManagerClassPath;
        this.remoteLogMetadataManagerClassName = remoteLogMetadataManagerClassName;
        this.remoteLogMetadataManagerClassPath = remoteLogMetadataManagerClassPath;
        this.remoteLogIndexFileCacheTotalSizeBytes = remoteLogIndexFileCacheTotalSizeBytes;
        this.remoteLogManagerThreadPoolSize = remoteLogManagerThreadPoolSize;
        this.remoteLogManagerTaskIntervalMs = remoteLogManagerTaskIntervalMs;
        this.remoteLogManagerTaskRetryBackoffMs = remoteLogManagerTaskRetryBackoffMs;
        this.remoteLogManagerTaskRetryBackoffMaxMs = remoteLogManagerTaskRetryBackoffMaxMs;
        this.remoteLogManagerTaskRetryJitterMs = remoteLogManagerTaskRetryJitterMs;
        this.remoteLogReaderThreads = remoteLogReaderThreads;
        this.remoteLogReaderMaxPendingTasks = remoteLogReaderMaxPendingTasks;
        this.remoteStorageManagerProps = new HashMap<>(remoteStorageManagerProps);
        this.remoteLogMetadataManagerProps = new HashMap<>(remoteLogMetadataManagerProps);
        this.remoteLogMetadataManagerListenerName = remoteLogMetadataManagerListenerName;
    }

    public boolean enableRemoteStorageSystem() {
        return enableRemoteStorageSystem;
    }

    public String remoteStorageManagerClassName() {
        return remoteStorageManagerClassName;
    }

    public String remoteStorageManagerClassPath() {
        return remoteStorageManagerClassPath;
    }

    public String remoteLogMetadataManagerClassName() {
        return remoteLogMetadataManagerClassName;
    }

    public String remoteLogMetadataManagerClassPath() {
        return remoteLogMetadataManagerClassPath;
    }

    public long remoteLogIndexFileCacheTotalSizeBytes() {
        return remoteLogIndexFileCacheTotalSizeBytes;
    }

    public int remoteLogManagerThreadPoolSize() {
        return remoteLogManagerThreadPoolSize;
    }

    public long remoteLogManagerTaskIntervalMs() {
        return remoteLogManagerTaskIntervalMs;
    }

    public long remoteLogManagerTaskRetryBackoffMs() {
        return remoteLogManagerTaskRetryBackoffMs;
    }

    public long remoteLogManagerTaskRetryBackoffMaxMs() {
        return remoteLogManagerTaskRetryBackoffMaxMs;
    }

    public long remoteLogManagerTaskRetryJitterMs() {
        return remoteLogManagerTaskRetryJitterMs;
    }

    public int remoteLogReaderThreads() {
        return remoteLogReaderThreads;
    }

    public int remoteLogReaderMaxPendingTasks() {
        return remoteLogReaderMaxPendingTasks;
    }

    public String remoteLogMetadataManagerListenerName() {
        return remoteLogMetadataManagerListenerName;
    }

    public HashMap<String, Object> remoteStorageManagerProps() {
        return remoteStorageManagerProps;
    }

    public HashMap<String, Object> remoteLogMetadataManagerProps() {
        return remoteLogMetadataManagerProps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteLogManagerConfig)) return false;
        RemoteLogManagerConfig that = (RemoteLogManagerConfig) o;
        return enableRemoteStorageSystem == that.enableRemoteStorageSystem
                && remoteLogIndexFileCacheTotalSizeBytes == that.remoteLogIndexFileCacheTotalSizeBytes
                && remoteLogManagerThreadPoolSize == that.remoteLogManagerThreadPoolSize
                && remoteLogManagerTaskIntervalMs == that.remoteLogManagerTaskIntervalMs
                && remoteLogManagerTaskRetryBackoffMs == that.remoteLogManagerTaskRetryBackoffMs
                && remoteLogManagerTaskRetryBackoffMaxMs == that.remoteLogManagerTaskRetryBackoffMaxMs
                && remoteLogManagerTaskRetryJitterMs == that.remoteLogManagerTaskRetryJitterMs
                && remoteLogReaderThreads == that.remoteLogReaderThreads
                && remoteLogReaderMaxPendingTasks == that.remoteLogReaderMaxPendingTasks
                && Objects.equals(remoteStorageManagerClassName, that.remoteStorageManagerClassName)
                && Objects.equals(remoteStorageManagerClassPath, that.remoteStorageManagerClassPath)
                && Objects.equals(remoteLogMetadataManagerClassName, that.remoteLogMetadataManagerClassName)
                && Objects.equals(remoteLogMetadataManagerClassPath, that.remoteLogMetadataManagerClassPath)
                && Objects.equals(remoteLogMetadataManagerListenerName, that.remoteLogMetadataManagerListenerName)
                && Objects.equals(remoteStorageManagerProps, that.remoteStorageManagerProps)
                && Objects.equals(remoteLogMetadataManagerProps, that.remoteLogMetadataManagerProps);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(enableRemoteStorageSystem, remoteStorageManagerClassName, remoteStorageManagerClassPath, remoteLogMetadataManagerClassName,
                      remoteLogMetadataManagerClassPath, remoteLogMetadataManagerListenerName, remoteLogIndexFileCacheTotalSizeBytes,
                      remoteLogManagerThreadPoolSize, remoteLogManagerTaskIntervalMs, remoteLogManagerTaskRetryBackoffMs,
                      remoteLogManagerTaskRetryBackoffMaxMs, remoteLogManagerTaskRetryJitterMs, remoteLogReaderThreads,
                      remoteLogReaderMaxPendingTasks, remoteStorageManagerProps, remoteLogMetadataManagerProps);
    }
}