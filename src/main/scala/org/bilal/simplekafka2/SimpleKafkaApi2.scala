package org.bilal.simplekafka2

import io.bullet.borer.Json
import org.bilal.json.{Codecs, Serde}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.server.Config
import org.dist.simplekafka.LeaderAndReplicaRequest

class SimpleKafkaApi2(config:Config, replicaManager: ReplicaManager2) extends Codecs{
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
    }
  }
}
