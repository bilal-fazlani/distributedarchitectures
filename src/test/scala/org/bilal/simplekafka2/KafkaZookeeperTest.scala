package org.bilal.simplekafka2

import org.bilal.codec.Codecs
import org.bilal.simplekafka2.KafkaZookeeper.ControllerExists
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, Matchers}

class KafkaZookeeperTest
    extends ZookeeperTestHarness
    with Matchers
    with Codecs
    with Eventually
    with BeforeAndAfterEach {

  private var zookeeperScala: ZookeeperScala = _
  private var kafkaZookeeper: KafkaZookeeper = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    zookeeperScala = new ZookeeperScala(zkClient)

    kafkaZookeeper = new KafkaZookeeper(
      zookeeperScala,
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
    kafkaZookeeper.registerBroker(Broker(1, "node1", 8080))
    val broker:Broker = zookeeperScala.readData[Broker]("/brokers/ids/1")
    broker should ===(Broker(1,"node1", 8080))
  }

  test("should register self"){
    kafkaZookeeper.registerSelf()
    val broker:Broker = zookeeperScala.readData[Broker]("/brokers/ids/9")
    broker should ===(Broker(9,"localhost", 8000))
  }

  test("should get all broker ids") {
    kafkaZookeeper.registerBroker(Broker(1, "node1", 1000))
    kafkaZookeeper.registerBroker(Broker(2, "node2", 2000))
    kafkaZookeeper.registerBroker(Broker(3, "node3", 3000))
    val ids: Set[Int] = kafkaZookeeper.allBrokerIds()
    ids shouldBe (Set(1, 2, 3))
  }

  test("should get all brokers") {
    kafkaZookeeper.registerBroker(Broker(1, "node1", 1000))
    kafkaZookeeper.registerBroker(Broker(2, "node2", 2000))
    kafkaZookeeper.registerBroker(Broker(3, "node3", 3000))
    val brokers: Set[Broker] = kafkaZookeeper.allBrokers()
    brokers shouldBe (Set(
      Broker(1, "node1", 1000),
      Broker(2, "node2", 2000),
      Broker(3, "node3", 3000),
    ))
  }

  test("should get broker information for given id") {
    kafkaZookeeper.registerBroker(Broker(1, "node1", 1000))
    val broker: Broker = kafkaZookeeper.getBrokerInfo(1)
    broker shouldBe Broker(1, "node1", 1000)
  }

  test("should partition the topic evenly across brokers") {
    kafkaZookeeper.registerBroker(Broker(1, "localhost", 9000))
    kafkaZookeeper.registerBroker(Broker(2, "localhost2", 9090))
    kafkaZookeeper.registerBroker(Broker(3, "localhost3", 8000))
    kafkaZookeeper.createTopic("topic1", 2, 2)
    val data: Set[PartitionReplicas] =
      zookeeperScala.readData[Set[PartitionReplicas]]("/brokers/topics/topic1")
    data should ===(Set(PartitionReplicas(1, List(1, 2)), PartitionReplicas(2, List(3, 1))))
  }

  test("should get partition assignments for topic") {
    kafkaZookeeper.registerBroker(Broker(1, "localhost", 9000))
    kafkaZookeeper.registerBroker(Broker(2, "localhost2", 9090))
    kafkaZookeeper.registerBroker(Broker(3, "localhost3", 8000))
    kafkaZookeeper.createTopic("topic1", 2, 2)
    kafkaZookeeper.getPartitionAssignmentsForTopic("topic1") should
      ===(Set(PartitionReplicas(1, List(1, 2)), PartitionReplicas(2, List(3, 1))))
  }

  test("should become controller when no on is controller"){
    kafkaZookeeper.tryToBeController(1) should ===(Right(()))
  }

  test("should not become controller when there is already a controller"){
    kafkaZookeeper.tryToBeController(2)
    kafkaZookeeper.tryToBeController(2) should ===(Left(ControllerExists(2)))
  }

  test("should subscribe to topic changes"){
    kafkaZookeeper.registerBroker(Broker(1, "node1", 1000))
    kafkaZookeeper.registerBroker(Broker(2, "node2", 1000))
    var actualTopics = Set.empty[String]
    kafkaZookeeper.subscribeToTopicChanges{ topics =>
      actualTopics = topics
    }
    kafkaZookeeper.createTopic("topic1", 1, 1)
    kafkaZookeeper.createTopic("topic2", 1, 1)
    eventually {
      actualTopics should ===(Set("topic1", "topic2"))
    }
  }

  test("should subscriber to broker changes"){
    var actualIds = Set.empty[Int]
    kafkaZookeeper.registerBroker(Broker(1, "node1", 1000))
    kafkaZookeeper.subscribeToBrokerChanges(ids => {
      actualIds = ids
    })
    kafkaZookeeper.registerBroker(Broker(2, "node2", 1000))
    eventually{
      actualIds should ===(Set(1, 2))
    }
  }

  test("should subscriber to controller changes"){
    var actualResult:Option[Int] = None
    kafkaZookeeper.subscriberControllerChanges(actualResult = _)
    kafkaZookeeper.tryToBeController(10)
    eventually{
      actualResult should ===(Some(10))
    }
  }
}
