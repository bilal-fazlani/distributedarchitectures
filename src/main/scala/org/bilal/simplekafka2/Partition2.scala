package org.bilal.simplekafka2

import java.nio.file.Paths

import io.bullet.borer.{Codec, Encoder}
import org.bilal.simplekafka2.Partition2.Record
import org.bilal.simplekafka2.SequenceFile2.Offset
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config

class Partition2(config:Config, topicPartition: TopicAndPartition) {
  def makeFollower(leaderId: Int) = ()

  def makeLeader() = ()

  private val fileName = s"${topicPartition.topic}-${topicPartition.partition}.log"
  private val fileUri = Paths.get(config.logDirs.head, fileName)
  val file = new SequenceFile2(fileUri.toAbsolutePath.toUri)

  def appendMessage[T:Encoder](key:String, message:T): Offset =file.appendMessage(key, message)

  def read[T:Codec](offset: Offset = 0L):Set[Record[T]] = {
    val nextOffsets = file.getAllOffsetsFrom(offset)
    nextOffsets.map(offset => {
      file.readMessage[T](offset)
    })
  }
}
object Partition2{
  case class Record[T:Codec](key:String, value:T)
}
