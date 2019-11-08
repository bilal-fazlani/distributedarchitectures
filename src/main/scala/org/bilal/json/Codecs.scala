package org.bilal.json

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs._
import org.bilal.simplekafka2.Partition2.Record
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicaRequest, PartitionReplicas}

import scala.reflect.ClassTag

trait Codecs {
  implicit lazy val partitionReplicasCodec: Codec[PartitionReplicas] = deriveCodec
  implicit lazy val brokerCodec: Codec[Broker] = deriveCodec
  implicit lazy val stringCodec:Codec[String] = io.bullet.borer.Codec.of[String]
  implicit lazy val leaderAndReplicaRequestCodec:Codec[LeaderAndReplicaRequest] = io.bullet.borer.Codec.of[LeaderAndReplicaRequest]
  implicit def recordCodec[T:Codec:ClassTag]:Codec[Record[T]] = deriveCodec
  implicit def setCodec[T:Codec:ClassTag]:Codec[Set[T]] = Codec.bimap[Array[T], Set[T]](_.toArray, _.toSet)
}
