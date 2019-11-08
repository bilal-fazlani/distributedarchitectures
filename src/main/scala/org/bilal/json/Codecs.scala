package org.bilal.json

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs._
import org.bilal.api2.Request2.{Consume2, GetTopicMetadata2, LeaderAndReplica, Produce, UpdateMetadata}
import org.bilal.api2.Response2.{ConsumeResponse2, GetTopicMetadataResponse2, LeaderAndReplicaResponse2, ProduceResponse2, UpdateMetadataResponse2}
import org.bilal.api2.{Request2, Response2}
import org.bilal.simplekafka2.Partition2.Record
import org.bilal.simplekafka2.SequenceFile2.Offset
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ConsumeRequest, ConsumeResponse, LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, PartitionReplicas, ProduceRequest, ProduceResponse, TopicMetadataRequest, TopicMetadataResponse, UpdateMetadataRequest}

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

  implicit lazy val leaderAndReplicaCodec:Codec[LeaderAndReplica] = deriveCodec
  implicit lazy val leaderAndReplicaResponseCodec:Codec[LeaderAndReplicaResponse2] = deriveCodec
  implicit lazy val updateMetadataRequestCodec:Codec[UpdateMetadataRequest] = deriveCodec

  implicit lazy val topicMetadataRequestCodec:Codec[TopicMetadataRequest] = deriveCodec
  implicit lazy val getTopicMetadataRequestCodec:Codec[GetTopicMetadata2] = deriveCodec
  implicit lazy val getTopicMetadataResponseCodec:Codec[GetTopicMetadataResponse2] = deriveCodec

  implicit lazy val produceRequestCodec:Codec[ProduceRequest] = deriveCodec

  implicit lazy val consumeRequestCodec:Codec[ConsumeRequest] = deriveCodec
  implicit lazy val consumeResponseCodec:Codec[ConsumeResponse2] = deriveCodec

  implicit lazy val produce2Codec:Codec[Produce] = deriveCodec
  implicit lazy val produceResponseCodec:Codec[ProduceResponse2] = deriveCodec
  implicit lazy val consume2Codec:Codec[Consume2] = deriveCodec

  implicit lazy val request2Codec:Codec[Request2] = deriveCodec
  implicit lazy val response2Codec:Codec[Response2] = deriveCodec

  implicit lazy val updateMeta2Codec:Codec[UpdateMetadata] = deriveCodec
  implicit lazy val updateMetaResponseCodec:Codec[UpdateMetadataResponse2] = deriveCodec

  implicit def recordCodec[T:Codec:ClassTag]:Codec[Record[T]] = deriveCodec
  implicit def setCodec[T:Codec:ClassTag]:Codec[Set[T]] = Codec.bimap[Array[T], Set[T]](_.toArray, _.toSet)
}
