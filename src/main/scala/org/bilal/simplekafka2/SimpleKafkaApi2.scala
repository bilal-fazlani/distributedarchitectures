package org.bilal.simplekafka2

import org.bilal.json.{Codecs, Serde}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka._

class SimpleKafkaApi2(config:Config, replicaManager: ReplicaManager2) extends Codecs{

  var aliveBrokers = List[Broker]()
  var leaderCache = Map[TopicAndPartition, PartitionInfo]()

  def handle(request: RequestOrResponse):RequestOrResponse ={
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey =>
        val leaderAndReplicasRequest = Serde.decode[LeaderAndReplicaRequest](request.messageBodyJson.getBytes())
        leaderAndReplicasRequest.leaderReplicas.foreach(x => {
          val leader = x.partitionStateInfo.leader
          val topicPartition = x.topicPartition
          if(leader.id == config.brokerId)
            replicaManager.makeLeader(topicPartition)
          else replicaManager.makeFollower(topicPartition, leader.id)
        })
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
      case RequestKeys.UpdateMetadataKey => {
        val updateMetadataRequest = Serde.decode[UpdateMetadataRequest](request.messageBodyJson)
        aliveBrokers = updateMetadataRequest.aliveBrokers
        updateMetadataRequest.leaderReplicas.foreach(leaderReplica => {
          leaderCache = leaderCache + (leaderReplica.topicPartition -> leaderReplica.partitionStateInfo)
        })
        RequestOrResponse(RequestKeys.UpdateMetadataKey, "", request.correlationId)
      }
      case RequestKeys.GetMetadataKey => {
        val topicMetadataRequest = Serde.decode[TopicMetadataRequest](request.messageBodyJson)
        val topicAndPartitions = leaderCache.keys.filter(topicAndPartition => topicAndPartition.topic == topicMetadataRequest.topicName)
        val partitionInfo: Map[TopicAndPartition, PartitionInfo] = topicAndPartitions.map(tp => (tp, leaderCache(tp))).toMap
        val topicMetadata = TopicMetadataResponse(partitionInfo)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, Serde.encodeToString(topicMetadata), request.correlationId)
      }
      case RequestKeys.ProduceKey => {
        val produceRequest: ProduceRequest = Serde.decode[ProduceRequest](request.messageBodyJson)
        val partition = replicaManager.getPartition(produceRequest.topicAndPartition)
        val offset = partition.appendMessage(produceRequest.key, produceRequest.message)
        RequestOrResponse(RequestKeys.ProduceKey,Serde.encodeToString(ProduceResponse(offset)) ,request.correlationId)
      }
      case RequestKeys.FetchKey => {
        val consumeRequest = Serde.decode[ConsumeRequest](request.messageBodyJson)
        val partition = replicaManager.getPartition(consumeRequest.topicAndPartition)
        val rows = partition.read[String](consumeRequest.offset)
        val consumeResponse = ConsumeResponse(rows.map(row => (row.key, row.value)).toMap)
        RequestOrResponse(RequestKeys.FetchKey, Serde.encodeToString(consumeResponse), request.correlationId)
      }
      case _ => RequestOrResponse(0, "Unknown Request", request.correlationId)
    }
  }
}
