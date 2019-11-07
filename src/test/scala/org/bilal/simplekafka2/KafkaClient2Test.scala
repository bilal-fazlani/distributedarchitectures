package org.bilal.simplekafka2

import org.bilal.json.Codecs
import org.bilal.simplekafka2.KafkaClient2.ControllerExists
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, Matchers}

class KafkaClient2Test
    extends ZookeeperTestHarness
    with Matchers
    with Codecs
    with Eventually
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
    val broker:Broker = zooKeeperClient.readData[Broker]("/brokers/ids/1")
    broker should ===(Broker(1,"node1", 8080))
  }

  test("should register self"){
    kafkaClient.registerSelf()
    val broker:Broker = zooKeeperClient.readData[Broker]("/brokers/ids/9")
    broker should ===(Broker(9,"localhost", 8000))
  }

  test("should get all broker ids") {
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    kafkaClient.registerBroker(Broker(2, "node2", 2000))
    kafkaClient.registerBroker(Broker(3, "node3", 3000))
    val ids: Set[Int] = kafkaClient.allBrokerIds()
    ids shouldBe (Set(1, 2, 3))
  }

  test("should get all brokers") {
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    kafkaClient.registerBroker(Broker(2, "node2", 2000))
    kafkaClient.registerBroker(Broker(3, "node3", 3000))
    val brokers: Set[Broker] = kafkaClient.allBrokers()
    brokers shouldBe (Set(
      Broker(1, "node1", 1000),
      Broker(2, "node2", 2000),
      Broker(3, "node3", 3000),
    ))
  }

  test("should get broker information for given id") {
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    val broker: Broker = kafkaClient.getBrokerInfo(1)
    broker shouldBe Broker(1, "node1", 1000)
  }

  test("should partition the topic evenly across brokers") {
    kafkaClient.registerBroker(Broker(1, "localhost", 9000))
    kafkaClient.registerBroker(Broker(2, "localhost2", 9090))
    kafkaClient.registerBroker(Broker(3, "localhost3", 8000))
    kafkaClient.createTopic("topic1", 2, 2)
    val data: Set[PartitionReplicas] =
      zooKeeperClient.readData[Set[PartitionReplicas]]("/brokers/topics/topic1")
    data should ===(Set(PartitionReplicas(1, List(1, 2)), PartitionReplicas(2, List(3, 1))))
  }

  test("should get partition assignments for topic") {
    kafkaClient.registerBroker(Broker(1, "localhost", 9000))
    kafkaClient.registerBroker(Broker(2, "localhost2", 9090))
    kafkaClient.registerBroker(Broker(3, "localhost3", 8000))
    kafkaClient.createTopic("topic1", 2, 2)
    kafkaClient.getPartitionAssignmentsForTopic("topic1") should
      ===(Set(PartitionReplicas(1, List(1, 2)), PartitionReplicas(2, List(3, 1))))
  }

  test("should become controller when no on is controller"){
    kafkaClient.tryToBeController(1) should ===(Right(()))
  }

  test("should not become controller when there is already a controller"){
    kafkaClient.tryToBeController(2)
    kafkaClient.tryToBeController(2) should ===(Left(ControllerExists(2)))
  }

  test("should subscribe to topic changes"){
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    kafkaClient.registerBroker(Broker(2, "node2", 1000))
    var actualTopics = Set.empty[String]
    kafkaClient.subscribeToTopicChanges{ topics =>
      actualTopics = topics
    }
    kafkaClient.createTopic("topic1", 1, 1)
    kafkaClient.createTopic("topic2", 1, 1)
    eventually {
      actualTopics should ===(Set("topic1", "topic2"))
    }
  }

  test("should subscriber to broker changes"){
    var actualIds = Set.empty[Int]
    kafkaClient.registerBroker(Broker(1, "node1", 1000))
    kafkaClient.subscribeToBrokerChanges(ids => {
      actualIds = ids
    })
    kafkaClient.registerBroker(Broker(2, "node2", 1000))
    eventually{
      actualIds should ===(Set(1, 2))
    }
  }

  test("should subscriber to controller changes"){
    var actualResult:Option[Int] = None
    kafkaClient.subscriberControllerChanges(actualResult = _)
    kafkaClient.tryToBeController(10)
    eventually{
      actualResult should ===(Some(10))
    }
  }
}
