package org.bilal.simplekafka2

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionInfo
import org.dist.util.Networks
import org.scalatest.{FunSuite, Matchers}

class ProducerConsumerTest
    extends FunSuite
    with Matchers
    with ZookeeperTestHarness {

  test("should produce and consumer messages from five broker cluster") {
    val broker1 = newBroker(1)
    val broker2 = newBroker(2)
    val broker3 = newBroker(3)
    val broker4 = newBroker(4)
    val broker5 = newBroker(5)

    broker1.start() //broker1 will become controller as its the first one to start
    broker2.start()
    broker3.start()
    broker4.start()
    broker5.start()

    TestUtils.waitUntilTrue(() => {
      broker1.controller.liveBrokers.size == 5
    }, "Waiting for all brokers to be discovered by the controller")

    broker1.kafkaZookeeper.createTopic("topic1", 2, 5)

    TestUtils.waitUntilTrue(() => {
      liveBrokersIn(broker1) == 5 && liveBrokersIn(broker2) == 5 && liveBrokersIn(broker3) == 5
    }, "waiting till topic metadata is propogated to all the servers", 2000)

    assert(leaderCache(broker1) == leaderCache(broker2) && leaderCache(broker2) == leaderCache(broker3))


    val bootstrapBroker = InetAddressAndPort.create(broker2.kafkaZookeeper.config.hostName, broker2.kafkaZookeeper.config.port)
    ???
  }

  private def leaderCache(broker: Server2): Map[TopicAndPartition, PartitionInfo] = {
//    broker.socketServer.kafkaApis.leaderCache
    ???
  }

  private def liveBrokersIn(broker1: Server2) = {
//    broker1.socketServer.kafkaApis.aliveBrokers.size
    0
  }

  private def newBroker(brokerId: Int): Server2 = {
    val config = Config(brokerId, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperScala = new ZookeeperScala(zkClient)
    val kafkaZookeeper: KafkaZookeeper = new KafkaZookeeper(zookeeperScala, config)
    val replicaManager = new ReplicaManager2(config)
//    val socketServer1 = new SimpleSocketServer(config.brokerId, config.hostName, config.port, new SimpleKafkaApi2(config, replicaManager))
    val controller = new Controller2(config.brokerId, kafkaZookeeper)
    new Server2(kafkaZookeeper, controller)
  }
}
