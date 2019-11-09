package org.bilal.simplekafka2

import org.bilal.simplekafka2.api.Request2.GetTopicMetadata2
import org.bilal.simplekafka2.api.{Request2, Response2}
import org.bilal.simplekafka2.api.Response2.GetTopicMetadataResponse2
import org.bilal.simplekafka2.codec.Codecs
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
      .sendReceiveTcp[Request2, GetTopicMetadataResponse2](GetTopicMetadata2(topic), from)
    response.topicPartitions
  }

  protected def partitionIdFor(key: String, numPartitions: Int): Int = {
    (Utils.abs(key.hashCode) % numPartitions) + 1
  }
}
