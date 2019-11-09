package org.bilal.simplekafka2

import org.bilal.simplekafka2.codec.Codecs
import org.bilal.simplekafka2.Partition2.Record
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.scalatest.FunSuite

class Partition2Test extends FunSuite with ZookeeperTestHarness with Codecs {

  test("testRead") {}

  test("testAppendMessage") {
    val partition = new Partition2(
      Config(
        100,
        "localhost",
        8000,
        zkConnect,
        List(TestUtils.tempDir().getAbsolutePath)
      ),
      TopicAndPartition("topic1", 2)
    )

    val offset1 = partition.appendMessage("key1", "hello")
    println(offset1)
    val offset2 = partition.appendMessage("key2", "world")
    println(offset2)
    val records = partition.read[String](0L)
    val records2 = partition.read[String](0L)
    println(records,records2)
  }

}
