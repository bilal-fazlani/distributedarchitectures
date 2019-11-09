package org.bilal.simplekafka2

import org.bilal.api.Request2.GetTopicMetadata2
import org.bilal.api.{Request2, Response2}
import org.bilal.api.Response2.GetTopicMetadataResponse2
import org.bilal.codec.Codecs
import org.bilal.remote.TcpClient
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.Utils
import org.dist.simplekafka.PartitionInfo

trait SimpleKafkaClient extends Codecs {
  protected def fetchMeta(
                           topic: String,
                           from: (String, Int)
                         ): Map[TopicAndPartition, PartitionInfo] = {
    val response = TcpClient
      .sendReceiveTcp[Request2, Response2](GetTopicMetadata2(topic), from)
      .asInstanceOf[GetTopicMetadataResponse2]
    response.topicPartitions
  }

  protected def partitionIdFor(key: String, numPartitions: Int): Int = {
    (Utils.abs(key.hashCode) % numPartitions) + 1
  }
}
