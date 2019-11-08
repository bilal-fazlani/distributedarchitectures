package org.bilal.remote

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicBoolean

import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.remote.TcpListener2.RequestHandler

import scala.util.control.NonFatal

class TcpListener2(requestHandler: RequestHandler)
    extends Thread
    with Codecs {
  val isRunning = new AtomicBoolean(true)
  var serverSocket: ServerSocket = _

  def shutdown(): Unit =
    try {
      serverSocket.close()
    } catch {
      case NonFatal(err) => err.printStackTrace()
    }

  override def run(): Unit = {
    try {
      serverSocket = new ServerSocket()
      serverSocket.bind(new InetSocketAddress(0))
      while (true) {
        val socket = serverSocket.accept()
        new SocketIO2[Request2, Response2](socket)
          .readHandleRespond(request => requestHandler(request))
      }
    } catch {
      case NonFatal(err) =>
        err.printStackTrace()
    }
    finally {
      serverSocket.close()
    }
  }
}
object TcpListener2{
  type RequestHandler = Request2 => Response2
}
