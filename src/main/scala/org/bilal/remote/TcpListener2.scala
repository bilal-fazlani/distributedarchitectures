package org.bilal.remote

import java.net.{InetSocketAddress, ServerSocket, SocketException}

import io.bullet.borer.Codec
import org.bilal.codec.Codecs

import scala.util.control.NonFatal

class TcpListener2[A:Codec, B:Codec](requestHandler: A => B, selfPort:Int)
    extends Thread
    with Codecs {

  val serverSocket: ServerSocket = new ServerSocket()

  def shutdown(): Unit =
    try {
      serverSocket.close()
    } catch {
      case NonFatal(err) => err.printStackTrace()
    }

  override def run(): Unit = {
    try {
      serverSocket.bind(new InetSocketAddress(selfPort))
      while (true) {
        val socket = serverSocket.accept()
        new SocketIO2[A, B](socket)
          .readHandleRespond(request => requestHandler(request))
      }
    } catch {
      case NonFatal(err:SocketException) if err.getMessage == "Socket closed" =>
      case NonFatal(err) => err.printStackTrace()
    }
  }
}
