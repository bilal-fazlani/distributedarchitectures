package org.bilal.remote

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicBoolean

import org.bilal.api2.{Request2, Response2}
import org.bilal.json.Codecs
import org.bilal.simplekafka2.SimpleKafkaApi2
import org.dist.kvstore.InetAddressAndPort

import scala.util.control.NonFatal

class TcpListener2(localEp: InetAddressAndPort,
                   kafkaApis: SimpleKafkaApi2,
                   socketServer: SimpleSocketServer2)
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
      serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
      while (true) {
        val socket = serverSocket.accept()
        new SocketIO2[Request2, Response2](socket)
          .readHandleRespond(request => kafkaApis.handle(request))
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
