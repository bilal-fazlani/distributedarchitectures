package org.bilal.simplekafka2

import java.util.concurrent.ConcurrentHashMap

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config

class ReplicaManager2(config:Config) {
  var allPartitions = new ConcurrentHashMap[TopicAndPartition, Partition2]()

  def makeLeader(topicAndPartition: TopicAndPartition):Unit = {
    getOrCreatePartition(topicAndPartition)
  }

  def makeFollower(topicAndPartition: TopicAndPartition, leaderId: Int):Unit =
    getOrCreatePartition(topicAndPartition)

  def getPartition(topicAndPartition: TopicAndPartition): Partition2 =
    allPartitions.get(topicAndPartition)

  private def getOrCreatePartition(topicAndPartition: TopicAndPartition): Partition2 =
    allPartitions.computeIfAbsent(topicAndPartition, new Partition2(config, _))
}
