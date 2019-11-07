package org.dist.simplekafka2


import java.util

import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.dist.kvstore.JsonSerDes
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
class ZookeeperClient2(zkClient: ZkClient) {

  def readData[T](path:String)(implicit ct: ClassTag[T]):T = decode(zkClient.readData(path))

  private def decode[T](str:String)(implicit ct: ClassTag[T]):T = {
    val x = JsonSerDes.deserialize(str.getBytes(), ct.runtimeClass)
    x.asInstanceOf[T]
  }

  def allChildren(path:String): Set[Int] =
    zkClient.getChildren(path).asScala.map(_.toInt).toSet

  def createEphemeralPath[T](path:String, data:T): Unit ={
    try{
      zkClient.createEphemeral(path, JsonSerDes.serialize(data))
    }
    catch {
      case _: ZkNoNodeException =>
        createParent(getParent(path))
        createEphemeralPath(path, data)
    }
  }

  def createPersistantPath[T](path:String): Unit = createPersistantPathMain(path, None)

  def createPersistantPath[T](path:String, data:T): Unit =
    createPersistantPathMain(path, Some(data))

  def subscribeChildChanges(path:String)(handler: (List[String]) => Unit): List[String] ={
    val result = zkClient.subscribeChildChanges(path, (_: String, currentChilds: util.List[String]) => {
      handler(currentChilds.asScala.toList)
    })
    result.asScala.toList
  }

  @scala.annotation.tailrec
  private def createPersistantPathMain[T](path:String, dataMaybe:Option[T]): Unit = {
    dataMaybe match {
      case Some(data) =>
        try{
          zkClient.createEphemeral(path, JsonSerDes.serialize(data))
        }
        catch {
          case _: ZkNoNodeException =>
            createParent(getParent(path))
            createPersistantPathMain(path, Some(data))
        }
      case None =>
        try{
          zkClient.createEphemeral(path)
        }
        catch {
          case _: ZkNoNodeException =>
            createParent(getParent(path))
            createPersistantPathMain(path, None)
        }
    }
  }

  private def createParent(parentPath:String): Unit = zkClient.createPersistent(parentPath, true)

  private def getParent(path:String):String={
    val lastIndex = path.lastIndexOf("/")
    path.substring(0, lastIndex)
  }
}
