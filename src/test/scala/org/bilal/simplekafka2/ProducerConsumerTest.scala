package org.bilal.simplekafka2

import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.remote.TcpServer
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class ProducerConsumerTest
    extends FunSuite
    with Matchers
    with MockitoSugar
    with Codecs
    with ZookeeperTestHarness {

  test("should produce and consumer messages from five broker cluster") {
    val (broker1, kafkaApi1) = newBroker(1)
    val (broker2, kafkaApi2) = newBroker(2)
    val (broker3, kafkaApi3) = newBroker(3)
    val (broker4, kafkaApi4) = newBroker(4)
    val (broker5, kafkaApi5) = newBroker(5)

    broker1
      .start() //broker1 will become controller as its the first one to start
    broker2.start()
    broker3.start()
    broker4.start()
    broker5.start()

    TestUtils.waitUntilTrue(() => broker1.controller.liveBrokers.size == 5,
      "Waiting for all brokers to be discovered by the controller"
    )

    broker1.kafkaZookeeper.createTopic("topic1", 2, 5)

    TestUtils.waitUntilTrue(() =>
      (
        kafkaApi1.aliveBrokers.size == 5 &&
          kafkaApi2.aliveBrokers.size == 5 &&
          kafkaApi3.aliveBrokers.size == 5 &&
          kafkaApi4.aliveBrokers.size == 5 &&
          kafkaApi5.aliveBrokers.size == 5
      ),
      "waiting till topic metadata is propagated to all the servers"
    )

    assert(
      kafkaApi1.leaderCache == kafkaApi2.leaderCache &&
        kafkaApi2.leaderCache == kafkaApi3.leaderCache &&
        kafkaApi3.leaderCache == kafkaApi4.leaderCache &&
        kafkaApi4.leaderCache == kafkaApi5.leaderCache
    )
  }
  private def newBroker(brokerId: Int): (Server2, SimpleKafkaApi2) = {
    val networks = new Networks()
    val host = networks.hostname()
    val port = TestUtils.choosePort()
    val config = Config(
      brokerId,
      host,
      port,
      zkConnect,
      List(TestUtils.tempDir().getAbsolutePath)
    )
    val zookeeperScala = new ZookeeperScala(zkClient)
    val kafkaZookeeper: KafkaZookeeper =
      new KafkaZookeeper(zookeeperScala, config)
    val replicaManager = new ReplicaManager2(config)
    val kafkaApi = new SimpleKafkaApi2(config, replicaManager)
    val controller = new Controller2(config.brokerId, kafkaZookeeper)
    val tcpServer = new TcpServer[Request2, Response2](kafkaApi.handle, port)
    (new Server2(kafkaZookeeper, controller, tcpServer), kafkaApi)
  }
}
