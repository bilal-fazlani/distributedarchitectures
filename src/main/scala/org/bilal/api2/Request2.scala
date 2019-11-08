package org.bilal.api2

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.LeaderAndReplicas

sealed abstract class Request2(correlationId: String)
object Request2 {
  case class Produce(correlationId: String,
                     topicAndPartition: TopicAndPartition,
                     key: String,
                     message: String)
      extends Request2(correlationId)

  case class Consume2(correlationId: String,
                      topicAndPartition: TopicAndPartition,
                      offset: Long = 0)
    extends Request2(correlationId)

  case class LeaderAndReplica(correlationId: String,
                              leaderReplicas: List[LeaderAndReplicas])
      extends Request2(correlationId)

  case class UpdateMetadata(correlationId: String,
                            aliveBrokers: List[Broker],
                            leaderReplicas: List[LeaderAndReplicas])
      extends Request2(correlationId)

  case class GetTopicMetadata2(correlationId: String, topicName: String)
      extends Request2(correlationId)

}
