package org.bilal.api2

import org.bilal.simplekafka2.SequenceFile2.Offset
import org.dist.queue.common.TopicAndPartition
import org.dist.simplekafka.PartitionInfo

sealed abstract class Response2(correlationId: String)
object Response2 {
  case class ProduceResponse2(correlationId: String, offset: Offset)
      extends Response2(correlationId)

  case class ConsumeRequest2(correlationId: String,
                             topicAndPartition: TopicAndPartition,
                             offset: Long = 0)
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