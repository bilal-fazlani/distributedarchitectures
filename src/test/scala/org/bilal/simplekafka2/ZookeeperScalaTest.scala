package org.bilal.simplekafka2

import io.bullet.borer.Codec
import org.dist.queue.ZookeeperTestHarness
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import io.bullet.borer.derivation.MapBasedCodecs._

class ZookeeperScalaTest
    extends FunSuite
    with ZookeeperTestHarness
    with Matchers
    with Eventually
    with BeforeAndAfterEach {
  var zookeeperScala: ZookeeperScala = _

  implicit val testCodec: Codec[TestClass] = deriveCodec

  override def beforeEach(): Unit = {
    super.beforeEach()
    zookeeperScala = new ZookeeperScala(zkClient)
  }

  case class TestClass(number: Int)

  test("test AllChildren") {
    zookeeperScala.createPersistantPath("/abc/1")
    zookeeperScala.createPersistantPath("/abc/2")
    zookeeperScala.createPersistantPath("/abc/3")
    zookeeperScala.allChildren("/abc") should ===(Set(1, 2, 3))
  }

  test("test CreatePersistantPath") {
    zookeeperScala.createPersistantPath("/abc")
    zkClient.exists("/abc") shouldBe true
  }

  test("test CreatePersistantPathWithData") {
    zookeeperScala.createPersistantPath("/abc", TestClass(42))
    zkClient.exists("/abc") shouldBe true
    zookeeperScala.readData[TestClass]("/abc") should ===(TestClass(42))
  }

  test("test CreateEphemeralPath and readData") {
    zookeeperScala.createEphemeralPath("/root/node", TestClass(99))
    zookeeperScala.readData("/root/node") should ===(TestClass(99))
  }

  test("test SubscriberDataChanges") {

    var testData: Option[TestClass] = None

    zookeeperScala.subscriberDataChanges[TestClass]("/root/node1") {
      case Some(newData) => testData = Some(newData)
      case None          =>
    }

    zookeeperScala.createPersistantPath("/root/node1", TestClass(7))

    eventually {
      testData should ===(Some(TestClass(7)))
    }
  }

  test("test SubscribeChildChanges") {
    var set = Set.empty[String]
    zookeeperScala.subscribeChildChanges("/a") { ids =>
      set = ids
    }

    zookeeperScala.createEphemeralPath("/a/1", TestClass(1))
    zookeeperScala.createEphemeralPath("/a/2", TestClass(2))

    eventually {
      set should ===(Set("1", "2"))
    }
  }
}
