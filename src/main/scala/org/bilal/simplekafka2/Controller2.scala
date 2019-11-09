package org.bilal.simplekafka2

import org.bilal.simplekafka2.api.Request2.{LeaderAndReplica, UpdateMetadata}
import org.bilal.simplekafka2.api.{Request2, Response2}
import org.bilal.simplekafka2.codec.Codecs
import org.bilal.remote.TcpClient
import org.bilal.simplekafka2.KafkaZookeeper.ControllerExists
import org.bilal.simplekafka2.api.Response2.{LeaderAndReplicaResponse2, UpdateMetadataResponse2}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicas, PartitionInfo, PartitionReplicas}

import scala.collection.mutable

class Controller2(brokerId: Int, kafkaZookeeper: KafkaZookeeper)
    extends Codecs {
  var liveBrokers: Set[Broker] = Set()
  var currentController: Int = -1

  def start(): Unit = {
    kafkaZookeeper.subscriberControllerChanges {
      case Some(newController) =>
        currentController = newController
      case None =>
        electController()
    }
    electController()
  }

  private def electController(): Unit = {
    kafkaZookeeper.tryToBeController(brokerId) match {
      case Right(_) =>
        currentController = brokerId
        onBecomingController()
      case Left(ControllerExists(controllerId)) =>
        currentController = controllerId
    }
  }

  private def onBecomingController(): Unit = {
    liveBrokers = liveBrokers ++ kafkaZookeeper.allBrokers
    kafkaZookeeper.subscribeToTopicChanges(topics => {
      topics.foreach(t => {
        val assignments = kafkaZookeeper.getPartitionAssignmentsForTopic(t)
        val leadersAndReplicas =
          selectLeaderAndFollowerBrokersForPartitions(t, assignments)
        sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(
          leadersAndReplicas,
          assignments
        )
        sendUpdateMetadataRequestToAllLiveBrokers(leadersAndReplicas)
      })
    })
    kafkaZookeeper.subscribeToBrokerChanges(brokerIds => {
      liveBrokers = brokerIds.map(kafkaZookeeper.getBrokerInfo)
    })
  }

  private def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(
    leaderAndReplicas: Set[LeaderAndReplicas],
    partitionReplicas: Set[PartitionReplicas]
  ): Unit = {
    val brokerToLeaderIsrRequest =
      mutable.Map[Broker, mutable.ListBuffer[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr => {
      lr.partitionStateInfo.allReplicas.foreach(broker => {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker).orNull
        if (leaderReplicas == null) {
          leaderReplicas = mutable.ListBuffer[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.append(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet
    for (broker <- brokers) {
      val leaderAndReplicas = brokerToLeaderIsrRequest(broker)
      val request = LeaderAndReplica(leaderAndReplicas.toList)
      TcpClient.sendReceiveTcp[Request2, LeaderAndReplicaResponse2](
          request,
          (broker.host, broker.port)
        )
    }
  }
  private def sendUpdateMetadataRequestToAllLiveBrokers(
    leaderAndReplicas: Set[LeaderAndReplicas]
  ): Unit = {
    liveBrokers.foreach(broker => {
      val request = UpdateMetadata(liveBrokers.toList, leaderAndReplicas.toList)
      TcpClient.sendReceiveTcp[Request2, UpdateMetadataResponse2](
        request,
        (broker.host, broker.port)
      )
    })
  }

  private def selectLeaderAndFollowerBrokersForPartitions(
    topicName: String,
    partitionReplicas: Set[PartitionReplicas]
  ): Set[LeaderAndReplicas] = {
    partitionReplicas.map(pr => {
      val leaderId = pr.brokerIds.head
      val leaderBroker = getBroker(leaderId)
      val allReplicas = pr.brokerIds.map(getBroker)
      LeaderAndReplicas(
        TopicAndPartition(topicName, pr.partitionId),
        PartitionInfo(leaderBroker, allReplicas)
      )
    })
  }

  private def getBroker(brokerId: Int): Broker = {
    liveBrokers.find(b => b.id == brokerId).get
  }
}
