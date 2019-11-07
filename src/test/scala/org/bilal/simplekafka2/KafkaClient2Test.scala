package org.bilal.simplekafka2

import org.bilal.json.Codecs
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.scalatest.{BeforeAndAfterEach, Matchers}

class KafkaClient2Test
    extends ZookeeperTestHarness
    with Matchers
    with Codecs
    with BeforeAndAfterEach {

  private var zooKeeperClient: ZookeeperClient2 = _
  private var kafkaClient: KafkaClient2 = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    zooKeeperClient = new ZookeeperClient2(zkClient)

    kafkaClient = new KafkaClient2(
      zooKeeperClient,
      Config(
        9,
        "localhost",
        8000,
        zkConnect,
        List(TestUtils.tempDir().getAbsolutePath)
      )
    )
  }

  test("should register broker information in ephemeral node") {
    kafkaClient.registerBroker(Broker(1, "node1", 8080))
    assert(zkClient.exists("/brokers/ids/1"))
  }

  test("should get all broker ids") {
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    kafkaClient.registerBroker(Broker(2, "node2", 2000))
    kafkaClient.registerBroker(Broker(3, "node3", 3000))
    val ids: Set[Int] = kafkaClient.allBrokerIds()
    ids shouldBe (Set(1, 2, 3))
  }

  test("should partition the topic evenly across brokers") {
    kafkaClient.registerBroker(Broker(1, "localhost", 9000))
    kafkaClient.registerBroker(Broker(2, "localhost2", 9090))
    kafkaClient.registerBroker(Broker(3, "localhost3", 8000))
    kafkaClient.createTopic("topic1", 2, 2)
    val data: Set[PartitionReplicas] =
      zooKeeperClient.readData[Set[PartitionReplicas]]("/brokers/topics/topic1")
    data.foreach(println)
    println(data)
  }
}
