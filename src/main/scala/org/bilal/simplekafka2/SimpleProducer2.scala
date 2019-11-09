package org.bilal.simplekafka2

import org.bilal.api.Request2.Produce
import org.bilal.api.Response2.ProduceResponse2
import org.bilal.api.{Request2, Response2}
import org.bilal.extentions.Extensions._
import org.bilal.remote.TcpClient
import org.dist.queue.common.TopicAndPartition
import org.dist.simplekafka.PartitionInfo

class SimpleProducer2(bootstrapBrokerAddress: (String, Int)) extends SimpleKafkaClient {
  def produce(topic: String, key: String, message: String): Long = {
    val topicMetadata: Map[TopicAndPartition, PartitionInfo] = fetchMeta(topic, bootstrapBrokerAddress)
    require(topicMetadata.nonEmpty, s"fetched metadata size is 0 for topic $topic")
    val targetPartitionId = partitionIdFor(key, topicMetadata.size)
    val topicPartition = TopicAndPartition(topic, targetPartitionId)
    println(topicMetadata)
    println(topicPartition)
    val leaderBroker = topicMetadata(topicPartition).leader
    TcpClient
      .sendReceiveTcp[Request2, Response2](
        Produce(topicPartition, key, message),
        leaderBroker.targetAddress
      )
      .asInstanceOf[ProduceResponse2]
      .offset
  }
}
