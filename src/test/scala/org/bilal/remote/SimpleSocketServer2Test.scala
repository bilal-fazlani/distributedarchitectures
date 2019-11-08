package org.bilal.remote

import java.net.ConnectException

import org.bilal.codec.Codecs
import org.dist.queue.ZookeeperTestHarness
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSuite, Matchers}

class SimpleSocketServer2Test
    extends FunSuite
    with Matchers
    with Codecs
    with Eventually
    with ZookeeperTestHarness
    with MockitoSugar {

  test("should send message and receive response then shutdown"){
    val handler1 = (request:String) => request.toUpperCase

    val port1 = 5676
    val socketServer1 = new SimpleSocketServer2[String,String](port1,handler1)

    socketServer1.start()

    val send: String => String =
      socketServer1.sendReceiveTcp(_, ("localhost", port1))

    val x = send("hello")
    println(x)
    x should ===("HELLO")

    val y = send("world")
    println(y)
    y should ===("WORLD")

    socketServer1.shutdown()

    val error = intercept[ConnectException]{
      send("xyz")
    }

    error.getMessage should ===("Connection refused (Connection refused)")
  }
}
