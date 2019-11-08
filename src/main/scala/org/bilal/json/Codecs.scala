package org.bilal.json

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs._
import org.bilal.simplekafka2.Partition2.Record
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, PartitionReplicas, TopicMetadataResponse}

import scala.reflect.ClassTag

trait Codecs {
  implicit lazy val partitionReplicasCodec: Codec[PartitionReplicas] = deriveCodec
  implicit lazy val brokerCodec: Codec[Broker] = deriveCodec
  implicit lazy val stringCodec:Codec[String] = io.bullet.borer.Codec.of[String]

  implicit lazy val leaderAndReplicasCodec:Codec[LeaderAndReplicas] = deriveCodec
  implicit lazy val leaderAndReplicaRequestCodec:Codec[LeaderAndReplicaRequest] = deriveCodec
  implicit lazy val topicMetadataResponseCodec:Codec[TopicMetadataResponse] = deriveCodec
  implicit lazy val topicAndPartitionCodec:Codec[TopicAndPartition] = deriveCodec
  implicit lazy val partitionInfoCodec:Codec[PartitionInfo] = deriveCodec

  implicit def recordCodec[T:Codec:ClassTag]:Codec[Record[T]] = deriveCodec
  implicit def setCodec[T:Codec:ClassTag]:Codec[Set[T]] = Codec.bimap[Array[T], Set[T]](_.toArray, _.toSet)
}
