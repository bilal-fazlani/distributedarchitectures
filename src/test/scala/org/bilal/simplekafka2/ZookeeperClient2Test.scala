package org.bilal.simplekafka2

import io.bullet.borer.Codec
import org.dist.queue.ZookeeperTestHarness
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class ZookeeperClient2Test
    extends FunSuite
    with ZookeeperTestHarness
    with Matchers
    with Eventually
    with BeforeAndAfterEach {
  var zookeeperClient: ZookeeperClient2 = _
  import io.bullet.borer.derivation.MapBasedCodecs._
  implicit val testCodec: Codec[TestClass] = deriveCodec

  override def beforeEach(): Unit = {
    super.beforeEach()
    zookeeperClient = new ZookeeperClient2(zkClient)
  }

  case class TestClass(number: Int)

  test("test AllChildren") {
    zookeeperClient.createPersistantPath("/abc/1")
    zookeeperClient.createPersistantPath("/abc/2")
    zookeeperClient.createPersistantPath("/abc/3")
    zookeeperClient.allChildren("/abc") should ===(Set(1, 2, 3))
  }

  test("test CreatePersistantPath") {
    zookeeperClient.createPersistantPath("/abc")
    zkClient.exists("/abc") shouldBe true
  }

  test("test CreatePersistantPathWithData") {
    zookeeperClient.createPersistantPath("/abc", TestClass(42))
    zkClient.exists("/abc") shouldBe true
    zookeeperClient.readData[TestClass]("/abc") should ===(TestClass(42))
  }

  test("test CreateEphemeralPath and readData") {
    zookeeperClient.createEphemeralPath("/root/node", TestClass(99))
    zookeeperClient.readData("/root/node") should ===(TestClass(99))
  }

  test("test SubscriberDataChanges") {

    var testData: Option[TestClass] = None

    zookeeperClient.subscriberDataChanges[TestClass]("/root/node1") {
      case Some(newData) => testData = Some(newData)
      case None          =>
    }

    zookeeperClient.createPersistantPath("/root/node1", TestClass(7))

    eventually {
      testData should ===(Some(TestClass(7)))
    }
  }

  test("test SubscribeChildChanges") {
    var set = Set.empty[String]
    zookeeperClient.subscribeChildChanges("/a") { ids =>
      set = ids
    }

    zookeeperClient.createEphemeralPath("/a/1", TestClass(1))
    zookeeperClient.createEphemeralPath("/a/2", TestClass(2))

    eventually {
      set should ===(Set("1", "2"))
    }
  }
}
