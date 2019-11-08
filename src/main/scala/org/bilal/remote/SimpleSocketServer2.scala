package org.bilal.remote

import java.net.Socket

import io.bullet.borer.Codec
import org.bilal.codec.Codecs

import scala.util.Using

class SimpleSocketServer2[A: Codec, B: Codec](selfPort: Int, val requestHandler: A => B
) extends Codecs {

  def this(requestHandler: A => B) = this(0, requestHandler)

  var tcpListener: TcpListener2[A, B] = _

  def start(): Unit = {
    tcpListener = new TcpListener2(requestHandler, selfPort)
    tcpListener.start()
  }

  def shutdown(): Unit = tcpListener.shutdown()

  def sendReceiveTcp(request: A, to: (String, Int)): B = {
    Using.resource(new Socket(to._1, to._2)){ socket =>
      new SocketIO2[A, B](socket)
        .requestResponse(request)
    }
  }
}
