package org.bilal.remote

import java.net.Socket

import org.bilal.api2.{Request2, Response2}
import org.bilal.json.Codecs
import org.bilal.simplekafka2.SimpleKafkaApi2
import org.dist.kvstore.InetAddressAndPort

class SimpleSocketServer2(
                           val brokerId: Int,
                           val host: String,
                           val port: Int,
                           val kafkaApis: SimpleKafkaApi2
                         ) extends Codecs{
  var tcpListener:TcpListener2 = _

  def start(): Unit ={
    tcpListener = new TcpListener2(InetAddressAndPort.create(host, port), kafkaApis, this)
    tcpListener.start()
  }

  def shutdown(): Unit = tcpListener.shutdown()

  def sendReceiveTcp(request: Request2, to: InetAddressAndPort): Response2 ={
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO2[Request2, Response2](clientSocket)
      .requestResponse(request)
  }
}
