package org.bilal.remote

import java.net.ConnectException

import org.bilal.simplekafka2.codec.Codecs
import org.dist.queue.ZookeeperTestHarness
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSuite, Matchers}

class TcpClientServerTest
    extends FunSuite
    with Matchers
    with Codecs
    with Eventually
    with ZookeeperTestHarness
    with MockitoSugar {

  test("should send message and receive response"){
    val handler1 = (request:String) => request.toUpperCase

    val port = 5676
    val server = new TcpServer[String,String](handler1, port)

    server.start()

    val send: String => String =
      TcpClient.sendReceiveTcp[String,String](_, ("localhost", port))

    val x = send("hello")
    println(x)
    x should ===("HELLO")

    val y = send("world")
    println(y)
    y should ===("WORLD")

    server.shutdown()

    val error = intercept[ConnectException]{
      send("xyz")
    }

    error.getMessage should ===("Connection refused (Connection refused)")
  }
}
