package org.bilal.simplekafka2

import java.io.{File, IOException, RandomAccessFile}
import java.net.URI
import java.util.concurrent.atomic.AtomicLong

import io.bullet.borer.{Codec, Encoder}
import org.bilal.json.Serde
import org.bilal.simplekafka2.Partition2.Record
import org.bilal.simplekafka2.SequenceFile2.{Key, Offset, Position}

import scala.util.control.NonFatal

class SequenceFile2(fileName: URI) {
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
    try{
      val bytes = Serde.encode(message)
      if(key == null) throw new RuntimeException("key can not be null")
      if(message == null) throw new RuntimeException("message can not be null")

      val lastWritePositionCached = lastWriterPosition
      file.seek(lastWritePositionCached)
      file.writeUTF(key)
      val length = bytes.length
      file.writeInt(length)
      file.write(bytes, 0, length)
      file.getFD.sync()
      lastWriterPosition = file.getFilePointer
      keyIndex = keyIndex + (key -> lastWritePositionCached)
      val currentOffset = offset.incrementAndGet()
      offsetIndex = offsetIndex + (currentOffset -> lastWritePositionCached)
      currentOffset
    }
    catch {
      case NonFatal(err: IOException) =>
        file.seek(lastWriterPosition)
        throw err
    }
  }

  def readMessage[T:Codec](offset: Offset):Record[T] = {
    val lastPositionCached = file.getFilePointer
    try{
      val position = offsetIndex(offset)
      file.seek(position)
      val key = file.readUTF()
      val length = file.readInt()
      var bytes = new Array[Byte](length)
      file.read(bytes, 0, length)
      val message = Serde.decode[T](bytes)
      Record(key, message)
    }
    catch {
      case NonFatal(err) =>
        err.printStackTrace()
        throw err
    }
    finally {
      file.seek(lastPositionCached)
    }
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