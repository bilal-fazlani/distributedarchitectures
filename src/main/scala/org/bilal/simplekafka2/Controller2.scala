package org.bilal.simplekafka2

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicas, PartitionInfo, PartitionReplicas}
import org.bilal.simplekafka2.KafkaClient2.ControllerExists

class Controller2(brokerId:Int, kafkaClient: KafkaClient2) {
  var liveBrokers: Set[Broker] = Set()
  var currentController: Int = -1

  def start(): Unit = {
    kafkaClient.subscriberControllerChanges {
      case Some(newController) =>
        currentController = newController
      case None =>
        electController()
    }
    electController()
  }

  private def electController(): Unit = {
    kafkaClient.tryToBeController(brokerId) match {
      case Right(_) =>
        currentController = brokerId
        onBecomingLeader()
      case Left(ControllerExists(controllerId)) =>
        currentController = controllerId
    }
  }

  private def onBecomingLeader():Unit = {
    liveBrokers = liveBrokers ++ kafkaClient.allBrokers
    kafkaClient.subscribeToTopicChanges(topics => {
      topics.foreach(t => {
        val assignments = kafkaClient.getPartitionAssignmentsForTopic(t)
        val leadersAndReplicas = selectLeaderAndFollowerBrokersForPartitions(t,assignments)
        //fixme: send information using socket
        println(leadersAndReplicas)
      })
    })
    kafkaClient.subscribeToBrokerChanges(brokerIds => {
      liveBrokers = brokerIds.map(kafkaClient.getBrokerInfo)
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
