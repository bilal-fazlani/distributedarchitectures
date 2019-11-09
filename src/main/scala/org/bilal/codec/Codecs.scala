package org.bilal.codec

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs._
import org.bilal.api.Request2._
import org.bilal.api.Response2._
import org.bilal.api.{Request2, Response2}
import org.bilal.simplekafka2.Partition2.Record
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka._

import scala.reflect.ClassTag

trait Codecs {
  implicit lazy val partitionReplicasCodec: Codec[PartitionReplicas] = deriveCodec
  implicit lazy val brokerCodec: Codec[Broker] = deriveCodec
  implicit lazy val stringCodec:Codec[String] = io.bullet.borer.Codec.of[String]

  private implicit lazy val topicAndPartitionCodec:Codec[TopicAndPartition] = deriveCodec
  private implicit lazy val partitionInfoCodec:Codec[PartitionInfo] = deriveCodec

  //responses
  implicit lazy val produceResponseCodec:Codec[ProduceResponse2] = deriveCodec
  implicit lazy val consumeResponseCodec:Codec[ConsumeResponse2] = deriveCodec
  implicit lazy val getTopicMetadataResponseCodec:Codec[GetTopicMetadataResponse2] = deriveCodec
  implicit lazy val leaderAndReplicaResponseCodec:Codec[LeaderAndReplicaResponse2] = deriveCodec
  implicit lazy val updateMetaResponseCodec:Codec[UpdateMetadataResponse2] = deriveCodec
  implicit lazy val response2Codec:Codec[Response2] = deriveCodec
  //responses

  implicit lazy val request2Codec:Codec[Request2] = {
    implicit lazy val leaderAndReplicasCodec:Codec[LeaderAndReplicas] = deriveCodec
    implicit lazy val leaderAndReplicaCodec:Codec[LeaderAndReplica] = deriveCodec
    implicit lazy val getTopicMetadataRequestCodec:Codec[GetTopicMetadata2] = deriveCodec
    implicit lazy val produce2Codec:Codec[Produce] = deriveCodec
    implicit lazy val consume2Codec:Codec[Consume2] = deriveCodec
    implicit lazy val updateMeta2Codec:Codec[UpdateMetadata] = deriveCodec
    deriveCodec
  }

  implicit def recordCodec[T:Codec:ClassTag]:Codec[Record[T]] = deriveCodec
  implicit def setCodec[T:Codec:ClassTag]:Codec[Set[T]] = Codec.bimap[Array[T], Set[T]](_.toArray, _.toSet)
}