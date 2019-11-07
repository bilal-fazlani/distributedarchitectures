package org.bilal.simplekafka2

import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.bilal.json.Codecs
import org.bilal.simplekafka2.KafkaClient2.ControllerExists
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas

class KafkaClient2(zookeeperClient: ZookeeperClient2, config: Config)
    extends Codecs {
  private val brokerIdsPath = "/brokers/ids"
  private val topicsPath = "/brokers/topics"
  private val controllerPath = "/controller"
  private val self = Broker(config.brokerId, config.hostName, config.port)

  def allBrokers(): Set[Broker] =
    zookeeperClient
      .allChildren(brokerIdsPath)
      .map(getBrokerInfo)

  def getBrokerInfo(id: Int): Broker =
    zookeeperClient.readData[Broker](getBrokerPath(id))

  private def getBrokerPath(id: Int): String = s"$brokerIdsPath/$id"

  def createTopic(name: String,
                  noOfPartitions: Int,
                  replicationFactor: Int): Unit = {
    val allBrokers = allBrokerIds()
    val partitionReplicas: Set[PartitionReplicas] =
      createPartitionReplicasForBrokers(
        allBrokers,
        noOfPartitions,
        replicationFactor
      )
    zookeeperClient.createPersistantPath(getTopicPath(name), partitionReplicas)
  }

  private def getTopicPath(name: String): String = s"$topicsPath/$name"

  def allBrokerIds(): Set[Int] =
    zookeeperClient
      .allChildren(brokerIdsPath)

  private def createPartitionReplicasForBrokers(
    brokers: Set[Int],
    noOfPartitions: Int,
    replicationFactor: Int
  ): Set[PartitionReplicas] = {
    val numberOfBrokers: Int = brokers.size

    var partitionReplicas: Set[PartitionReplicas] = Set.empty

    var brokerId: Int = 1

    (1 to noOfPartitions).foreach(p => {
      var brokerList = List[Int]()
      (1 to replicationFactor).foreach { _: Int =>
        brokerList = brokerList :+ brokerId
        if (brokerId == numberOfBrokers)
          brokerId = 1
        else
          brokerId += 1
      }
      partitionReplicas += PartitionReplicas(p, brokerList)
    })
    partitionReplicas
  }

  def getPartitionAssignmentsForTopic(name: String): Set[PartitionReplicas] =
    zookeeperClient.readData[Set[PartitionReplicas]](getTopicPath(name))

  def subscribeToTopicChanges(handler: Set[String] => Unit): Set[String] =
    zookeeperClient.subscribeChildChanges(topicsPath)(handler)

  def subscribeToBrokerChanges(handler: Set[Int] => Unit): Set[String] =
    zookeeperClient.subscribeChildChanges(brokerIdsPath)(
      x => handler(x.map(_.toInt))
    )

  def tryToBeController(id: Int): Either[ControllerExists, Unit] = {
    try {
      zookeeperClient.createEphemeralPath(controllerPath, id.toString)
      Right(())
    } catch {
      case _: ZkNodeExistsException =>
        val controllerId = zookeeperClient.readData[String](controllerPath)
        Left(ControllerExists(controllerId.toInt))
    }
  }

  def subscriberControllerChanges(onChange: Option[Int] => Unit): Unit =
    zookeeperClient.subscriberDataChanges[String](controllerPath) {
      case Some(value) => onChange(Some(value.toInt))
      case None        => onChange(None)
    }

  def registerSelf(): Unit = registerBroker(self)

  def registerBroker(broker: Broker): Unit =
    zookeeperClient.createEphemeralPath(getBrokerPath(broker.id), broker)

  def shutdown(): Unit = zookeeperClient.shutdown()
}

case object KafkaClient2 {
  case class ControllerExists(controllerId: Int)
}
