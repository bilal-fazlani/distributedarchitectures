package org.bilal.simplekafka2

import java.io.{File, RandomAccessFile}
import java.util.concurrent.atomic.AtomicLong

import io.bullet.borer.{Encoder, Json}
import org.bilal.simplekafka2.SequenceFile2.{Key, Offset, Position}

class SequenceFile2(fileName: String) {
  private var lastWriterPosition: Position = 0L
  private val file: RandomAccessFile = getOrCreateFile()
  private val offset = new AtomicLong(0L)
  private var keyIndex = Map[Key, Position]()
  private var offsetIndex = Map[Offset, Position]()

  def getAllOffsetsFrom(offset: Offset): Set[Offset] =
    offsetIndex.keys
      .filter(_ > offset)
      .toSet

  def appendMessage[T:Encoder](key:Key, message:T): Offset = {
    val bytes = Json.encode(message).to[Array[Byte]].result
    if(key == null) throw new RuntimeException("key can not be null")
    if(message == null) throw new RuntimeException("message can not be null")

    val lastWritePositionCached = lastWriterPosition
    file.seek(lastWritePositionCached)
    file.writeUTF(key)
    val length = bytes.length
    file.writeLong(length)
    file.write(bytes, 0, length)
    file.getFD.sync()
    lastWriterPosition = file.getFilePointer
    keyIndex = keyIndex + (key -> lastWritePositionCached)
    val currentOffset = offset.incrementAndGet()
    offsetIndex = offsetIndex + (currentOffset -> lastWritePositionCached)
    currentOffset
  }

  def position(key:Key): Position = keyIndex(key)

  def position(offset: Offset): Position = offsetIndex(offset)

  private def getOrCreateFile(): RandomAccessFile = {
    val file = new File(fileName)
    if(!file.exists()) file.createNewFile()
    new RandomAccessFile(file, "rw")
  }
}

object SequenceFile2{
  type Offset = Long
  type Position = Long
  type Key = String
}