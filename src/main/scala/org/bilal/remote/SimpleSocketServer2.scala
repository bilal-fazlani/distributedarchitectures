package org.bilal.remote

import java.net.Socket

import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.remote.TcpListener2.RequestHandler
import org.dist.kvstore.InetAddressAndPort

class SimpleSocketServer2(val requestHandler: RequestHandler)
    extends Codecs {
  var tcpListener: TcpListener2 = _

  def start(): Unit = {
    tcpListener =
      new TcpListener2(requestHandler)
    tcpListener.start()
  }

  def shutdown(): Unit = tcpListener.shutdown()

  def sendReceiveTcp(request: Request2, to: (String,Int)): Response2 = {
    val clientSocket = new Socket(to._1, to._2)
    new SocketIO2[Request2, Response2](clientSocket)
      .requestResponse(request)
  }
}
