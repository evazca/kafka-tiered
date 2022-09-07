# Introduction

This module provides HDFS implementation for `RemoteStorageManager`. It stores the respective logs of non-compacted 
topic partitions in the configured HDFS base directory.

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
retention.ms=259200000

remote.log.storage.manager.impl.prefix=remote.log.storage.
remote.log.storage.hdfs.base.dir=kafka-remote-storage
remote.log.storage.hdfs.user=kloak
remote.log.storage.hdfs.keytab.path=/etc/kafka/kloak.keytab
```

# Steps to setup HDFS, ZK and Kafka in local machine

### Setup HDFS
Download the latest [Hadoop distribution 3.3.1](https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz) and start it in standalone mode:
```shell
$ wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
$ tar -xf hadoop-3.3.1.tar.gz
$ cd hadoop-3.3.1
$ export HADOOP_HOME=`pwd`
```

Configure the core-site.xml and hdfs-site.xml files under ${HADOOP_HOME}/etc/hadoop folder:
```xml
<!-- core-site.xml -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

```xml
<!-- hdfs-site.xml -->
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/data/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/data/dfs/data</value>
  </property>
</configuration>
```

Start the HDFS name-node and data-node:
```shell
# format the namenode directory
bin/hdfs namenode -format
# starts the namenode
bin/hdfs namenode
# starts the datanode
bin/hdfs datanode

# create 'kafka-remote-storage' log directory under which the topic data gets saved
bin/hdfs dfs -mkdir -p /user/<username>/kafka-remote-storage
# To list all the files in HDFS
bin/hdfs dfs -ls -R /

# Sample commands to test the HDFS read/write operation
echo Hello | cat > /tmp/hello.txt
bin/hdfs dfs -copyFromLocal /tmp/hello.txt kafka-remote-storage/
bin/hdfs dfs -cat kafka-remote-storage/hello.txt

# To delete all the contents under 'kafka-remote-storage' directory:
./bin/hdfs dfs -rm -R -skipTrash kafka-remote-storage
```

### Setup ZK and Kafka

Checkout and build the 2.8.x-tiering-dev branch:
```shell
$ git clone gitolite@code.uber.internal:data/kafka
$ cd kafka; git fetch origin 2.8.x-tiering-dev; git checkout -b 2.8.x-tiering-dev FETCH_HEAD
$ ./gradlew clean releaseTarGz -x test
$ cd core/build/distributions/ 
$ tar -xf kafka_2.13-2.8.0.tgz
```

In server.properties, configure the below properties to enable HDFS as remote log storage in broker:
```properties
############################# Remote Storage Configurations ############################
# Remote Storage Manager Config
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
remote.log.storage.manager.class.path=<HADOOP_HOME>/etc/hadoop:<KAFKA_HOME>/external/hdfs/libs/*

remote.log.storage.manager.impl.prefix=remote.log.storage.
remote.log.storage.hdfs.base.dir=kafka-remote-storage
# remote.log.storage.hdfs.user=<user>
# remote.log.storage.hdfs.keytab.path=<user>.keytab

# Remote Log Metadata Manager Config
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.class.path=
remote.log.metadata.manager.listener.name=PLAINTEXT

remote.log.metadata.manager.impl.prefix=rlmm.config.
rlmm.config.remote.log.metadata.topic.num.partitions=5
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.retention.ms=-1
```

Run the ZooKeeper and Kafka server locally and create the topics with remote log storage:
```shell
$ cd <KAFKA_HOME>/bin; JMX_PORT=9990 nohup sh zookeeper-server-start.sh ../config/zookeeper.properties &
$ cd <KAFKA_HOME>; JMX_PORT=9991 nohup sh bin/kafka-server-start.sh config/server.properties &

# Remote storage should be enabled at system (cluster) level and at individual topic level
# The retention time for this topic is set to default 7 days. And, configured the local retention timeout to 60 seconds to quickly cleanup the log segments from local log directory. 
# Once the active segment gets rolled out, it takes 90 seconds (initial log cleaner thread delay is 30s) for the log cleaner thread to clean up the expired local log segments.  
$ sh kafka-topics.sh --bootstrap-server localhost:9092 --topic sample-tiered-storage-test --replication-factor 1 --partitions 5 --create --config local.retention.ms=60000 --config segment.bytes=1048576 --config remote.storage.enable=true

# Run producer perf script to generate load on the topic
$ echo "bootstrap.servers=localhost:9092\nacks=all" | cat > test-producer.properties

$ sh kafka-producer-perf-test.sh --topic sample-tiered-storage-test --num-records 100000 --throughput 100 --record-size 10240 --producer.config test-producer.properties

# To view the uploaded segments chronologically in HDFS:
$ bin/hdfs dfs -ls -R kafka-remote-storage/ | grep sample | sort -k6,7

# Verify the earliest and latest log offset using GetOffsetShell CLI tool once the local log segments are removed
# NOTE: This tool doesn't return the earliest local log offset (-3) at this time.
$ sh kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sample-tiered-storage-test --time <-1/-2>

# Consume from the beginning of the topic to verify that the consumers are able to read all the messages from remote and local storage:
$ sh kafka-console-consumer.sh --bootstrap-server localhost:9092 localhost:9092 --topic sample-tiered-storage-test --partition 0 --from-beginning --property print.offset=true --property print.value=false
```
