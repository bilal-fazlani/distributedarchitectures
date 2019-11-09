package org.bilal.simplekafka2

import org.bilal.api.Request2.Consume2
import org.bilal.api.Response2.ConsumeResponse2
import org.bilal.api.{Request2, Response2}
import org.bilal.remote.TcpClient
import org.dist.queue.common.TopicAndPartition
import org.dist.simplekafka.PartitionInfo
import org.bilal.extentions.Extensions._

class SimpleConsumer2(bootstrapBrokerAddress: (String, Int)) extends SimpleKafkaClient {
  def consume(topic:String): Map[String, String] ={
    val meta = fetchMeta(topic, bootstrapBrokerAddress)
    meta.flatMap {
      case (tp:TopicAndPartition,v:PartitionInfo) =>
        TcpClient.sendReceiveTcp[Request2, Response2](Consume2(tp), v.leader.targetAddress)
        .asInstanceOf[ConsumeResponse2]
        .messages
    }
  }
}
