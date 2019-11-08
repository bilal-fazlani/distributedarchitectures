package org.bilal.api

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.LeaderAndReplicas

import scala.util.Random

sealed trait Request2{
  def correlationId: String
}
object Request2 {
  private def id(): String = Random.nextString(10)
  case class Produce(topicAndPartition: TopicAndPartition,
                     key: String,
                     message: String,
                     correlationId:String = id()
                    )
      extends Request2

  case class Consume2(
                      topicAndPartition: TopicAndPartition,
                      offset: Long = 0,
                      correlationId:String = id()
                     )
    extends Request2

  case class LeaderAndReplica(leaderReplicas: List[LeaderAndReplicas],
                              correlationId:String = id())
      extends Request2

  case class UpdateMetadata(aliveBrokers: List[Broker],
                            leaderReplicas: List[LeaderAndReplicas],
                            correlationId:String = id())
      extends Request2

  case class GetTopicMetadata2(topicName: String,correlationId:String = id())
      extends Request2

}
