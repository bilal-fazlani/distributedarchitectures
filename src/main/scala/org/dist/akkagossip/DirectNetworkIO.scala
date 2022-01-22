package org.dist.akkagossip

import org.dist.patterns.common.InetAddressAndPort

class Message(val from:InetAddressAndPort){}


class DirectNetworkIO(var connections:Map[InetAddressAndPort, ClusterDaemon]) {
  val disconnections:scala.collection.mutable.Map[InetAddressAndPort, InetAddressAndPort] = scala.collection.mutable.Map()

  def disconnect(from:InetAddressAndPort, to:InetAddressAndPort): Unit ={
    disconnections.put(from, to)
  }

  def send(node: InetAddressAndPort, message: Message):Unit = {
    if (hasDisconnectionFor(node, message)) {
      return
    }

    connections.get(node) match {
      case Some(connection) => connection.receive(message)
      case None =>
    }
  }

  private def hasDisconnectionFor(node: InetAddressAndPort, message: Message) = {
    !disconnections.isEmpty &&
    disconnections.get(message.from).forall(_.eq(node))
  }
}
