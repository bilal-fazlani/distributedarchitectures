package org.bilal.simplekafka2

import java.util.concurrent.ConcurrentHashMap

import org.bilal.simplekafka2.api.Request2._
import org.bilal.simplekafka2.api.Response2._
import org.bilal.simplekafka2.api._
import org.bilal.simplekafka2.codec.Codecs
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka._

class SimpleKafkaApi2(config: Config, replicaManager: ReplicaManager2)
    extends Codecs {

  var aliveBrokers = List[Broker]()
  var leaderCache = Map[TopicAndPartition, PartitionInfo]()

  def handle(request: Request2): Response2 = {
    request match {
      case Produce(topicAndPartition, key, message, correlationId) =>
        val partition = replicaManager.getPartition(topicAndPartition)
        val offset = partition.appendMessage(key, message)
        ProduceResponse2(correlationId, offset)
      case Consume2(topicAndPartition, offset, correlationId) =>
        val partition =
          replicaManager.getPartition(topicAndPartition)
        val records = partition.read[String](offset)
        val data = records.map(record => (record.key, record.value)).toMap
        ConsumeResponse2(correlationId, data)
      case LeaderAndReplica(leaderReplicas, correlationId) =>
        leaderReplicas.foreach(x => {
          val leader = x.partitionStateInfo.leader
          val topicPartition = x.topicPartition
          if (leader.id == config.brokerId)
            replicaManager.makeLeader(topicPartition)
          else replicaManager.makeFollower(topicPartition, leader.id)
        })
        LeaderAndReplicaResponse2(correlationId)
      case UpdateMetadata(aliveBrokers, leaderReplicas, correlationId) =>
        this.aliveBrokers = aliveBrokers
        leaderReplicas.foreach(leaderReplica => {
          leaderCache = leaderCache + (leaderReplica.topicPartition -> leaderReplica.partitionStateInfo)
        })
        UpdateMetadataResponse2(correlationId)
      case GetTopicMetadata2(topicName, correlationId) =>
        val topicAndPartitions = leaderCache.keys.filter(
          topicAndPartition => topicAndPartition.topic == topicName
        )
        val partitionInfo: Map[TopicAndPartition, PartitionInfo] =
          topicAndPartitions.map(tp => (tp, leaderCache(tp))).toMap
        GetTopicMetadataResponse2(correlationId, partitionInfo)
    }
  }
}
