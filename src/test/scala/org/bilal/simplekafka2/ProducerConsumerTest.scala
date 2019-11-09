package org.bilal.simplekafka2

import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.remote.TcpServer
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks
import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class ProducerConsumerTest
    extends FunSuite
    with Matchers
    with MockitoSugar
    with Codecs
    with Eventually
    with ZookeeperTestHarness {

  test("should produce and consumer messages from five broker cluster") {
    val (broker1, kafkaApi1, address1) = newBroker(1)
    val (broker2, kafkaApi2, address2) = newBroker(2)
    val (broker3, kafkaApi3, address3) = newBroker(3)
    val (broker4, kafkaApi4, address4) = newBroker(4)
    val (broker5, kafkaApi5, address5) = newBroker(5)

    broker1.start() //broker1 will become controller as its the first one to start
    broker2.start()
    broker3.start()
    broker4.start()
    broker5.start()

    eventually{
      broker1.controller.liveBrokers should have size 5
    }

    broker1.kafkaZookeeper.createTopic("topic1", 2, 5)

    eventually {
      (kafkaApi1.aliveBrokers.size == 5 &&
        kafkaApi2.aliveBrokers.size == 5 &&
        kafkaApi3.aliveBrokers.size == 5 &&
        kafkaApi4.aliveBrokers.size == 5 &&
        kafkaApi5.aliveBrokers.size == 5) shouldBe true
    }

    assert(
      kafkaApi1.leaderCache == kafkaApi2.leaderCache &&
        kafkaApi2.leaderCache == kafkaApi3.leaderCache &&
        kafkaApi3.leaderCache == kafkaApi4.leaderCache &&
        kafkaApi4.leaderCache == kafkaApi5.leaderCache
    )

    val simpleProducer = new SimpleProducer2(address5)
    val offset1 = simpleProducer.produce("topic1", "key1", "message1")
    assert(offset1 == 1) //first offset

    val offset2 = simpleProducer.produce("topic1", "key2", "message2")
    assert(offset2 == 1) //first offset on different partition

    val offset3 = simpleProducer.produce("topic1", "key3", "message3")

    assert(offset3 == 2) //offset on first partition

    val simpleConsumer = new SimpleConsumer2(address2)
    val messages = simpleConsumer.consume("topic1")

    messages should ===(Map(
      "key1" -> "message1",
      "key2" -> "message2",
      "key3" -> "message3"
    ))
  }
  private def newBroker(brokerId: Int): (Server2, SimpleKafkaApi2, (String, Int)) = {
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
    (new Server2(kafkaZookeeper, controller, tcpServer), kafkaApi, (host, port))
  }
}
