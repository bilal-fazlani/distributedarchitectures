package org.bilal.simplekafka2

import org.bilal.api2._
import org.bilal.json.Codecs
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
      case Produce(correlationId, topicAndPartition, key, message) =>
        val partition = replicaManager.getPartition(topicAndPartition)
        val offset = partition.appendMessage(key, message)
        ProduceResponse2(correlationId, offset)
      case LeaderAndReplica(correlationId, leaderReplicas) =>
        leaderReplicas.foreach(x => {
          val leader = x.partitionStateInfo.leader
          val topicPartition = x.topicPartition
          if (leader.id == config.brokerId)
            replicaManager.makeLeader(topicPartition)
          else replicaManager.makeFollower(topicPartition, leader.id)
        })
        LeaderAndReplicaResponse2(correlationId)
      case UpdateMetadata(correlationId, aliveBrokers, leaderReplicas) =>
        this.aliveBrokers = aliveBrokers
        leaderReplicas.foreach(leaderReplica => {
          leaderCache = leaderCache + (leaderReplica.topicPartition -> leaderReplica.partitionStateInfo)
        })
        UpdateMetadataResponse2(correlationId)
      case GetTopicMetadata2(correlationId, topicName) =>
        val topicAndPartitions = leaderCache.keys.filter(
          topicAndPartition => topicAndPartition.topic == topicName
        )
        val partitionInfo: Map[TopicAndPartition, PartitionInfo] =
          topicAndPartitions.map(tp => (tp, leaderCache(tp))).toMap
        GetTopicMetadataResponse2(correlationId, partitionInfo)
      case ConsumeRequest2(correlationId, topicAndPartition, offset) =>
        val partition =
          replicaManager.getPartition(topicAndPartition)
        val records = partition.read[String](offset)
        val data = records.map(record => (record.key, record.value)).toMap
      ConsumeResponse2(correlationId, data)
    }
  }
}
