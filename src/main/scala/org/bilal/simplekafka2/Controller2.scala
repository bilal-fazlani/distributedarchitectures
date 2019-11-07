package org.bilal.simplekafka2

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicas, PartitionInfo, PartitionReplicas}
import org.bilal.simplekafka2.KafkaClient2.ControllerExists

class Controller2(brokerId:Int, kafkaClient: KafkaClient2) {

  var liveBrokers: Set[Broker] = Set()
  var currentController: Int = -1

  def startup(): Unit = {
    kafkaClient.subscriberControllerChanges {
      case Some(newController) =>
        currentController = newController.toInt
      case None =>
        electController()
    }
    electController()
  }

  def electController(): Unit = {
    kafkaClient.tryToBeController(brokerId.toString) match {
      case Left(_) =>
        currentController = brokerId
        onBecomingLeader()
      case Right(ControllerExists(controllerId)) =>
        currentController = controllerId.toInt
    }
  }

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

  private def getBroker(brokerId:Int): Broker = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }
}
