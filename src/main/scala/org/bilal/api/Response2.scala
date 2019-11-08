package org.bilal.api

import org.dist.queue.common.TopicAndPartition
import org.dist.simplekafka.PartitionInfo

sealed abstract class Response2(correlationId: String)
object Response2 {
  case class ProduceResponse2(correlationId: String, offset: Long)
      extends Response2(correlationId)

  case class LeaderAndReplicaResponse2(correlationId: String)
      extends Response2(correlationId)

  case class UpdateMetadataResponse2(correlationId: String,
                                     errorCode: Short = 0)
      extends Response2(correlationId)

  case class GetTopicMetadataResponse2(
    correlationId: String,
    topicPartitions: Map[TopicAndPartition, PartitionInfo]
  ) extends Response2(correlationId)

  case class ConsumeResponse2(correlationId: String,
                              messages: Map[String, String])
      extends Response2(correlationId)
}