package org.bilal.simplekafka2

import org.bilal.json.Codecs
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, Matchers}

class Controller2Test extends ZookeeperTestHarness
    with Matchers
    with Codecs
    with Eventually
    with BeforeAndAfterEach {

  private var zooKeeperClient: ZookeeperClient2 = _
  private def kafkaClient(brokerId:Int): KafkaClient2 = new KafkaClient2(
    zooKeeperClient,
    Config(
      brokerId,
      "localhost",
      8000,
      zkConnect,
      List(TestUtils.tempDir().getAbsolutePath)
    )
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    zooKeeperClient = new ZookeeperClient2(zkClient)
  }

  test("should start the controller") {
    val controller:Controller2 = new Controller2(10, kafkaClient(10))
    controller.currentController should ===(-1)
    controller.start()
    eventually{
      controller.currentController should ===(10)
    }
  }

  test("when current controller is deleted, it re-elect itself") {
    val controller20:Controller2 = new Controller2(20, kafkaClient(20))
    val controller30:Controller2 = new Controller2(30, kafkaClient(30))
    controller20.currentController should ===(-1)
    controller30.currentController should ===(-1)
    controller20.start()
    controller30.start()
    eventually{
      controller20.currentController should ===(20)
      controller30.currentController should ===(20)
    }
    zkClient.delete("/controller")
    eventually{
      controller20.currentController should ===(controller30.currentController)
      controller20.currentController should be > 19
      controller30.currentController should be > 19
    }
  }
}
