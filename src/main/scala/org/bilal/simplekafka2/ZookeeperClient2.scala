package org.bilal.simplekafka2


import java.util

import io.bullet.borer.{Codec, Json}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.bilal.json.Codecs

import scala.jdk.CollectionConverters._
class ZookeeperClient2(zkClient: ZkClient) extends Codecs{

  def shutdown(): Unit = zkClient.close()

  def readData[T:Codec](path:String):T = Json.decode(
    zkClient.readData[String](path).getBytes()
  ).to[T].value

  def allChildren(path:String): Set[Int] =
    zkClient.getChildren(path).asScala.map(_.toInt).toSet

  def createEphemeralPath[T:Codec](path:String, data:T): Unit ={
    try{
      zkClient.createEphemeral(path, Json.encode(data).toUtf8String)
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

  def createPersistantPath[T:Codec](path:String, data:T): Unit =
    {
      try{
        zkClient.createEphemeral(path, Json.encode(data).toUtf8String)
      }
      catch {
        case _: ZkNoNodeException =>
          createParent(getParent(path))
          createPersistantPath(path, data)
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
