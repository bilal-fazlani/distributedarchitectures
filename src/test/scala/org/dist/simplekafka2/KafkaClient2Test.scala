package org.dist.simplekafka2

import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka2.KafkaClient2.PartitionReplicas
import org.scalatest.Matchers

class KafkaClient2Test extends ZookeeperTestHarness with Matchers{
  private lazy val zooKeeperClient = new ZookeeperClient2(zkClient)
  private lazy val newKafka = new KafkaClient2(zooKeeperClient)

  test("should register broker information in ephemeral node"){
    val client = newKafka
    client.registerBroker(Broker(1, "node1", 8080))
    assert(zkClient.exists("/brokers/ids/1"))
  }

  test("should get all broker ids"){
    val client = newKafka
    client.registerBroker(Broker(1, "node1", 1000))
    client.registerBroker(Broker(2, "node2", 2000))
    client.registerBroker(Broker(3, "node3", 3000))
    val ids:Set[Int] = client.allBrokerIds()
    ids shouldBe(Set(1,2,3))
  }

  test("testCreateTopic") {
    val client = newKafka
    client.registerBroker(Broker(1, "localhost", 9000))
    client.registerBroker(Broker(2, "localhost2", 9090))
    client.registerBroker(Broker(3, "localhost3", 8000))
    client.createTopic("topic1", 2, 2)
    val data: Set[PartitionReplicas] = zooKeeperClient.readData[Set[PartitionReplicas]]("/brokers/topics/topic1")
    println(data)
  }
}
