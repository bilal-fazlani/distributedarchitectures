package org.dist.simplekafka2

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicas, PartitionInfo, PartitionReplicas}

class Controller2(zookeeperClient: ZookeeperClient2, kafkaClient: KafkaClient2) {

  var liveBrokers: Set[Broker] = Set()

  def onBecomingLeader():Unit = {
    liveBrokers = liveBrokers ++ kafkaClient.allBrokers
    kafkaClient.subscribeToTopicChanges(topics => {
      topics.foreach(t => {
        val assignments = kafkaClient.getPartitionAssignmentsForTopic(t)
        val leadersAndReplicas = selectLeaderAndFollowerBrokersForPartitions(t,assignments)
        println(leadersAndReplicas)
      })
    })

    kafkaClient.subscribeToBrokerChanges(brokerIds => {
      liveBrokers = brokerIds.map(kafkaClient.getBrokerInfo).toSet
    })
  }

  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Set[PartitionReplicas]):Set[LeaderAndReplicas] = {
    partitionReplicas.map(pr => {
      val leaderId = pr.brokerIds.head
      val leaderBroker = getBroker(leaderId)
      val allReplicas = pr.brokerIds.map(getBroker)
      LeaderAndReplicas(TopicAndPartition(topicName, pr.partitionId), PartitionInfo(leaderBroker, allReplicas))
    })
  }

  private def getBroker(brokerId:Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }
}
