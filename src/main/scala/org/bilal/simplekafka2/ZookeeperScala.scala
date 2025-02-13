package org.bilal.simplekafka2


import java.util

import io.bullet.borer.Codec
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.bilal.simplekafka2.codec.{Codecs, Serde}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
class ZookeeperScala(zkClient: ZkClient) extends Codecs{

  def shutdown(): Unit = zkClient.close()

  def readData[T:Codec](path:String):T =
    Serde.decode[T](zkClient.readData[Array[Byte]](path))

  def allChildren(path:String): Set[Int] = {
      try {
        zkClient.getChildren(path).asScala.map(_.toInt).toSet
      }
      catch {
        case NonFatal(_:ZkNoNodeException) => Set.empty
      }
    }

  def createEphemeralPath[T:Codec](path:String, data:T): Unit ={
    try{
      zkClient.createEphemeral(path, Serde.encode(data))
    }
    catch {
      case _: ZkNoNodeException =>
        createParent(getParent(path))
        createEphemeralPath(path, data)
    }
  }

  def createPersistantPath(path:String): Unit = {
    try{
      zkClient.createEphemeral(path)
    }
    catch {
      case _: ZkNoNodeException =>
        createParent(getParent(path))
        createPersistantPath(path)
    }
  }

  def createPersistentPath[T:Codec](path:String, data:T): Unit =
    {
      try{
        zkClient.createEphemeral(path, Serde.encode(data))
      }
      catch {
        case _: ZkNoNodeException =>
          createParent(getParent(path))
          createPersistentPath(path, data)
      }
    }

  def subscribeChildChanges(path:String)(handler: (Set[String]) => Unit): Set[String] ={
    val result = zkClient.subscribeChildChanges(path, (_: String, currentChilds: util.List[String]) => {
      handler(currentChilds.asScala.toSet)
    })
    Option(result).map(_.asScala.toSet).getOrElse(Set.empty)
  }

  def subscriberDataChanges[T:Codec](path:String)(onChange: Option[T] => Unit): Unit = {
    zkClient.subscribeDataChanges(path, new IZkDataListener {
      override def handleDataChange(dataPath: String, data: Any): Unit = {
        val newData = readData[T](dataPath)
        onChange(Some(newData))
      }

      override def handleDataDeleted(dataPath: String): Unit = {
        onChange(None)
      }
    })
  }

  private def createParent(parentPath:String): Unit = zkClient.createPersistent(parentPath, true)

  private def getParent(path:String):String={
    val lastIndex = path.lastIndexOf("/")
    path.substring(0, lastIndex)
  }
}
