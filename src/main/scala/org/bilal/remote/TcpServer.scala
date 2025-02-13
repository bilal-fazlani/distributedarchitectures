package org.bilal.remote

import java.net.{InetSocketAddress, ServerSocket, SocketException}

import io.bullet.borer.Codec
import org.bilal.simplekafka2.codec.Codecs

import scala.language.implicitConversions
import scala.util.control.NonFatal
class TcpServer[A:Codec](requestHandler: A => TcpResponse, val port:Int)
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
      serverSocket.bind(new InetSocketAddress(port))
      while (true) {
        val socket = serverSocket.accept()
        new TcpClient(socket)
          .readAndHandleRequestThenSendResponse[A](request => requestHandler(request))
      }
    } catch {
      case NonFatal(err:SocketException) if err.getMessage == "Socket closed" =>
      case NonFatal(err) => err.printStackTrace()
    }
  }
}
