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

import java.net.{InetAddress, UnknownHostException}
import java.util.Properties
import DynamicConfig.Broker._
import kafka.api.ApiVersion
import kafka.controller.KafkaController
import kafka.log.{Log, LogConfig}
import kafka.network.ConnectionQuotas
import kafka.security.CredentialProvider
import kafka.server.Constants._
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.metrics.Quota._
import org.apache.kafka.common.utils.Sanitizer

import scala.collection._
import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.util.Try

/**
  * The ConfigHandler is used to process config change notifications received by the DynamicConfigManager
  */
trait ConfigHandler {
  def processConfigChanges(entityName: String, value: Properties): Unit
}

/**
  * The TopicConfigHandler will process topic config changes in ZK.
  * The callback provides the topic name and the full properties set read from ZK
  */
class TopicConfigHandler(private val replicaManager: ReplicaManager,
                         kafkaConfig: KafkaConfig, val quotas: QuotaManagers, kafkaController: KafkaController) extends ConfigHandler with Logging  {

  private def updateLogConfig(topic: String,
                              topicConfig: Properties,
                              configNamesToExclude: Set[String]): Unit = {
    val logManager = replicaManager.logManager
    logManager.topicConfigUpdated(topic)
    val logs = logManager.logsByTopic(topic)
    if (logs.nonEmpty) {
      /* combine the default properties with the overrides in zk to create the new LogConfig */
      val props = new Properties()
      topicConfig.asScala.forKeyValue { (key, value) =>
        if (!configNamesToExclude.contains(key)) props.put(key, value)
      }
      val logConfig = LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
      val wasRemoteLogEnabledBeforeUpdate = logs.head.remoteLogEnabled()
      logs.foreach(_.updateConfig(logConfig))
      maybeBootstrapRemoteLogComponents(topic, logs, wasRemoteLogEnabledBeforeUpdate)
    }
  }

  private[server] def maybeBootstrapRemoteLogComponents(topic: String,
                                                        logs: Seq[Log],
                                                        wasRemoteLogEnabledBeforeUpdate: Boolean): Unit = {

    def maybeFetchTopicId(topic: String, logs: Seq[Log]): Map[String, Uuid] = {
      val topicId = replicaManager.zkClient.getOrElse(throw new ConfigException(LogConfig.RemoteLogStorageEnableProp,
        logs.head.remoteLogEnabled(), s"Error occurred while setting the configuration as ZkClient is missing"))
        .getTopicIdsForTopics(Predef.Set(topic))
        .filter(entry => entry._2 != Uuid.ZERO_UUID)
        .getOrElse(topic, throw new ConfigException(LogConfig.RemoteLogStorageEnableProp,
          logs.head.remoteLogEnabled(), s"Error occurred while setting the configuration due to unavailability of topic ids for $topic"))

      Map(topic -> topicId)
    }

    // In Kafka v2.8, there is a bug when reading the topicId from the Log. The first LeaderAndIsr notification
    // doesn't set the topic-id in the Log which results to Uuid.ZERO_UUID. This has been fixed in the trunk.
    // So, loading the topic-id from ZK directly.
    val topicIds = maybeFetchTopicId(topic, logs)

    val isRemoteLogEnabled = logs.head.remoteLogEnabled()
    // Topic configs gets updated incrementally. This check is added to prevent redundant updates.
    if (!wasRemoteLogEnabledBeforeUpdate && isRemoteLogEnabled) {
      val (leaderPartitions, followerPartitions) =
        logs.flatMap(log => replicaManager.onlinePartition(log.topicPartition)).partition(_.isLeader)
      replicaManager.remoteLogManager.foreach(rlm =>
        rlm.onLeadershipChange(leaderPartitions.toSet, followerPartitions.toSet, topicIds.asJava))
    } else if (wasRemoteLogEnabledBeforeUpdate && !isRemoteLogEnabled) {
      logs.map(log => {
        log.maybeIncrementLogStartOffsetAsRemoteLogStorageDisabled()
        log.topicPartition
      })
        .groupBy(tp => replicaManager.onlinePartition(tp).exists(_.isLeader))
        .foreach {
          case (isLeader, partitions) =>
            replicaManager.remoteLogManager.foreach(rlm => rlm.stopPartitions(partitions.toSet, isLeader,
              (tp, ex) => error(s"Error while stopping the partition: $tp, ex: $ex")))
        }
    }
  }

  def processConfigChanges(topic: String, topicConfig: Properties): Unit = {
    // Validate the configurations.
    val configNamesToExclude = excludedConfigs(topic, topicConfig)

    updateLogConfig(topic, topicConfig, configNamesToExclude)

    def updateThrottledList(prop: String, quotaManager: ReplicationQuotaManager) = {
      if (topicConfig.containsKey(prop) && topicConfig.getProperty(prop).length > 0) {
        val partitions = parseThrottledPartitions(topicConfig, kafkaConfig.brokerId, prop)
        quotaManager.markThrottled(topic, partitions)
        debug(s"Setting $prop on broker ${kafkaConfig.brokerId} for topic: $topic and partitions $partitions")
      } else {
        quotaManager.removeThrottle(topic)
        debug(s"Removing $prop from broker ${kafkaConfig.brokerId} for topic $topic")
      }
    }
    updateThrottledList(LogConfig.LeaderReplicationThrottledReplicasProp, quotas.leader)
    updateThrottledList(LogConfig.FollowerReplicationThrottledReplicasProp, quotas.follower)

    if (Try(topicConfig.getProperty(KafkaConfig.UncleanLeaderElectionEnableProp).toBoolean).getOrElse(false)) {
      kafkaController.enableTopicUncleanLeaderElection(topic)
    }
  }

  def parseThrottledPartitions(topicConfig: Properties, brokerId: Int, prop: String): Seq[Int] = {
    val configValue = topicConfig.get(prop).toString.trim
    ThrottledReplicaListValidator.ensureValidString(prop, configValue)
    configValue match {
      case "" => Seq()
      case "*" => AllReplicas
      case _ => configValue.trim
        .split(",")
        .map(_.split(":"))
        .filter(_ (1).toInt == brokerId) //Filter this replica
        .map(_ (0).toInt).toSeq //convert to list of partition ids
    }
  }

  def excludedConfigs(topic: String, topicConfig: Properties): Set[String] = {
    // Verify message format version
    Option(topicConfig.getProperty(LogConfig.MessageFormatVersionProp)).flatMap { versionString =>
      if (kafkaConfig.interBrokerProtocolVersion < ApiVersion(versionString)) {
        warn(s"Log configuration ${LogConfig.MessageFormatVersionProp} is ignored for `$topic` because `$versionString` " +
          s"is not compatible with Kafka inter-broker protocol version `${kafkaConfig.interBrokerProtocolVersionString}`")
        Some(LogConfig.MessageFormatVersionProp)
      } else
        None
    }.toSet
  }
}


/**
 * Handles <client-id>, <user> or <user, client-id> quota config updates in ZK.
 * This implementation reports the overrides to the respective ClientQuotaManager objects
 */
class QuotaConfigHandler(private val quotaManagers: QuotaManagers) {

  def updateQuotaConfig(sanitizedUser: Option[String], sanitizedClientId: Option[String], config: Properties): Unit = {
    val clientId = sanitizedClientId.map(Sanitizer.desanitize)
    val producerQuota =
      if (config.containsKey(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).toLong.toDouble, true))
      else
        None
    quotaManagers.produce.updateQuota(sanitizedUser, clientId, sanitizedClientId, producerQuota)
    val consumerQuota =
      if (config.containsKey(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).toLong.toDouble, true))
      else
        None
    quotaManagers.fetch.updateQuota(sanitizedUser, clientId, sanitizedClientId, consumerQuota)
    val requestQuota =
      if (config.containsKey(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG).toDouble, true))
      else
        None
    quotaManagers.request.updateQuota(sanitizedUser, clientId, sanitizedClientId, requestQuota)
    val controllerMutationQuota =
      if (config.containsKey(QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG))
        Some(new Quota(config.getProperty(QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG).toDouble, true))
      else
        None
    quotaManagers.controllerMutation.updateQuota(sanitizedUser, clientId, sanitizedClientId, controllerMutationQuota)
  }
}

/**
 * The ClientIdConfigHandler will process clientId config changes in ZK.
 * The callback provides the clientId and the full properties set read from ZK.
 */
class ClientIdConfigHandler(private val quotaManagers: QuotaManagers) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(sanitizedClientId: String, clientConfig: Properties): Unit = {
    updateQuotaConfig(None, Some(sanitizedClientId), clientConfig)
  }
}

/**
 * The UserConfigHandler will process <user> and <user, client-id> quota changes in ZK.
 * The callback provides the node name containing sanitized user principal, sanitized client-id if this is
 * a <user, client-id> update and the full properties set read from ZK.
 */
class UserConfigHandler(private val quotaManagers: QuotaManagers, val credentialProvider: CredentialProvider) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(quotaEntityPath: String, config: Properties): Unit = {
    // Entity path is <user> or <user>/clients/<client>
    val entities = quotaEntityPath.split("/")
    if (entities.length != 1 && entities.length != 3)
      throw new IllegalArgumentException("Invalid quota entity path: " + quotaEntityPath)
    val sanitizedUser = entities(0)
    val sanitizedClientId = if (entities.length == 3) Some(entities(2)) else None
    updateQuotaConfig(Some(sanitizedUser), sanitizedClientId, config)
    if (!sanitizedClientId.isDefined && sanitizedUser != ConfigEntityName.Default)
      credentialProvider.updateCredentials(Sanitizer.desanitize(sanitizedUser), config)
  }
}

class IpConfigHandler(private val connectionQuotas: ConnectionQuotas) extends ConfigHandler with Logging {

  def processConfigChanges(ip: String, config: Properties): Unit = {
    val ipConnectionRateQuota = Option(config.getProperty(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG)).map(_.toInt)
    val updatedIp = {
      if (ip != ConfigEntityName.Default) {
        try {
          Some(InetAddress.getByName(ip))
        } catch {
          case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ip")
        }
      } else
        None
    }
    connectionQuotas.updateIpConnectionRateQuota(updatedIp, ipConnectionRateQuota)
  }
}

/**
  * The BrokerConfigHandler will process individual broker config changes in ZK.
  * The callback provides the brokerId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ReplicationQuotaManager objects
  */
class BrokerConfigHandler(private val brokerConfig: KafkaConfig,
                          private val quotaManagers: QuotaManagers) extends ConfigHandler with Logging {

  def processConfigChanges(brokerId: String, properties: Properties): Unit = {
    def getOrDefault(prop: String): Long = {
      if (properties.containsKey(prop))
        properties.getProperty(prop).toLong
      else
        DefaultReplicationThrottledRate
    }
    if (brokerId == ConfigEntityName.Default)
      brokerConfig.dynamicConfig.updateDefaultConfig(properties)
    else if (brokerConfig.brokerId == brokerId.trim.toInt) {
      brokerConfig.dynamicConfig.updateBrokerConfig(brokerConfig.brokerId, properties)
      quotaManagers.leader.updateQuota(upperBound(getOrDefault(LeaderReplicationThrottledRateProp).toDouble))
      quotaManagers.follower.updateQuota(upperBound(getOrDefault(FollowerReplicationThrottledRateProp).toDouble))
      quotaManagers.alterLogDirs.updateQuota(upperBound(getOrDefault(ReplicaAlterLogDirsIoMaxBytesPerSecondProp).toDouble))
    }
  }
}

object ThrottledReplicaListValidator extends Validator {
  def ensureValidString(name: String, value: String): Unit =
    ensureValid(name, value.split(",").map(_.trim).toSeq)

  override def ensureValid(name: String, value: Any): Unit = {
    def check(proposed: Seq[Any]): Unit = {
      if (!(proposed.forall(_.toString.trim.matches("([0-9]+:[0-9]+)?"))
        || proposed.mkString.trim.equals("*")))
        throw new ConfigException(name, value,
          s"$name must be the literal '*' or a list of replicas in the following format: [partitionId]:[brokerId],[partitionId]:[brokerId],...")
    }
    value match {
      case scalaSeq: Seq[_] => check(scalaSeq)
      case javaList: java.util.List[_] => check(javaList.asScala)
      case _ => throw new ConfigException(name, value, s"$name must be a List but was ${value.getClass.getName}")
    }
  }

  override def toString: String = "[partitionId]:[brokerId],[partitionId]:[brokerId],..."

}
