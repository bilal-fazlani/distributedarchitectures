package org.dist.patterns.replicatedlog.heartbeat

import org.dist.kvstore.InetAddressAndPort
import org.dist.patterns.replicatedlog.Leader
import org.dist.rapid.SocketClient


case class Peer(id:Int, address:InetAddressAndPort)

case class PeerProxy(peerInfo: Peer, client: SocketClient = null, var matchIndex: Long = 0, heartbeatSender: PeerProxy ⇒ Unit) {

  def heartbeatSenderWrapper() = {
    heartbeatSender(this)
  }

  val heartBeat = new HeartBeatScheduler(heartbeatSenderWrapper)

  def start(): Unit = {
    heartBeat.start()
  }

  def stop() = {
    heartBeat.cancel()
  }
}
