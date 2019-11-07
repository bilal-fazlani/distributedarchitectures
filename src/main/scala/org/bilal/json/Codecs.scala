package org.bilal.json

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs._
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas

import scala.reflect.ClassTag

trait Codecs {
  implicit lazy val partitionReplicasCodec: Codec[PartitionReplicas] = deriveCodec
  implicit lazy val brokerCodec: Codec[Broker] = deriveCodec
  implicit lazy val stringCodec:Codec[String] = io.bullet.borer.Codec.of[String]
  implicit def setCodec[T:Codec:ClassTag]:Codec[Set[T]] = Codec.bimap[Array[T], Set[T]](_.toArray, _.toSet)
}
