package org.bilal.simplekafka2

import org.bilal.api.Request2.{GetTopicMetadata2, Produce}
import org.bilal.api.Response2.{GetTopicMetadataResponse2, ProduceResponse2}
import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.extentions.Extensions._
import org.bilal.remote.TcpClient
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.Utils
import org.dist.simplekafka.PartitionInfo

class SimpleProducer2(bootstrapBrokerAddress: (String, Int)) extends Codecs {

  def produce(topic: String, key: String, message: String): Long = {
    val topicMetadata: Map[TopicAndPartition, PartitionInfo] =
      fetchMeta(topic, bootstrapBrokerAddress)
    val targetPartitionId = partitionIdFor(key, topicMetadata.size)
    val topicPartition = TopicAndPartition(topic, targetPartitionId)
    val leaderBroker = topicMetadata(topicPartition).leader
    TcpClient
      .sendReceiveTcp[Request2, Response2](
        Produce(topicPartition, key, message),
        leaderBroker.targetAddress
      )
      .asInstanceOf[ProduceResponse2]
      .offset
  }

  private def fetchMeta(
    topic: String,
    from: (String, Int)
  ): Map[TopicAndPartition, PartitionInfo] = {
    val response = TcpClient
      .sendReceiveTcp[Request2, Response2](GetTopicMetadata2(topic), from)
      .asInstanceOf[GetTopicMetadataResponse2]
    response.topicPartitions
  }

  private def partitionIdFor(key: String, numPartitions: Int): Int = {
    Utils.abs(key.hashCode) % numPartitions
  }
}
