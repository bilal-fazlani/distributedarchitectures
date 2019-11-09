package org.bilal.simplekafka2

import org.bilal.api.{Request2, Response2}
import org.bilal.codec.Codecs
import org.bilal.remote.TcpServer
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas
import org.dist.util.Networks
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.concurrent.Eventually

class SimpleProducer2Test extends FunSuite with ZookeeperTestHarness with Codecs with Eventually with Matchers {

  test("testProduce") {
    val (leaderBroker, address) = newBroker(1)

    eventually {
      leaderBroker.kafkaZookeeper.allBrokers() should ===(Set(Broker(1, address._1, address._2)))
    }

    leaderBroker.kafkaZookeeper.createTopic("topic1", 1, 1)

    eventually{
      leaderBroker.kafkaZookeeper.getPartitionAssignmentsForTopic("topic1") should ===(
        Set(PartitionReplicas(1, List(1)))
      )
    }

    val producer = new SimpleProducer2(address)

    eventually{
      val offset = producer.produce("topic1", "test", "testMessage")
      println(offset)
      offset.toInt should be > -1
    }
  }


  private def newBroker(brokerId: Int): (Server2, (String, Int)) = {
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
    val server = new Server2(kafkaZookeeper, controller, tcpServer)
    server.start()
    (server, (host, port))
  }

}
