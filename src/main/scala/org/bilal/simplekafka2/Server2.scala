package org.bilal.simplekafka2

import org.bilal.simplekafka2.api.{Request2, Response2}
import org.bilal.remote.TcpServer

class Server2(val kafkaZookeeper: KafkaZookeeper,
              val controller: Controller2,
              val tcpServer: TcpServer[Request2, Response2]) {
  def start(): Unit = {
    tcpServer.start()
    println(s"TCP server started on ${tcpServer.port}")
    kafkaZookeeper.registerSelf()
    controller.start()
  }
  def shutdown(): Unit = {
    kafkaZookeeper.shutdown()
    tcpServer.shutdown()
  }
}
