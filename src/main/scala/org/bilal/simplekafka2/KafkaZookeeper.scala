package org.bilal.simplekafka2

import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.bilal.json.Codecs
import org.bilal.simplekafka2.KafkaZookeeper.ControllerExists
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas

class KafkaZookeeper(zookeeperScala: ZookeeperScala, val config: Config)
    extends Codecs {
  private val brokerIdsPath = "/brokers/ids"
  private val topicsPath = "/brokers/topics"
  private val controllerPath = "/controller"
  private val self = Broker(config.brokerId, config.hostName, config.port)

  def allBrokers(): Set[Broker] =
    zookeeperScala
      .allChildren(brokerIdsPath)
      .map(getBrokerInfo)

  def getBrokerInfo(id: Int): Broker =
    zookeeperScala.readData[Broker](getBrokerPath(id))

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
    zookeeperScala.createPersistantPath(getTopicPath(name), partitionReplicas)
  }

  private def getTopicPath(name: String): String = s"$topicsPath/$name"

  def allBrokerIds(): Set[Int] =
    zookeeperScala
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
    zookeeperScala.readData[Set[PartitionReplicas]](getTopicPath(name))

  def subscribeToTopicChanges(handler: Set[String] => Unit): Set[String] =
    zookeeperScala.subscribeChildChanges(topicsPath)(handler)

  def subscribeToBrokerChanges(handler: Set[Int] => Unit): Set[String] =
    zookeeperScala.subscribeChildChanges(brokerIdsPath)(
      x => handler(x.map(_.toInt))
    )

  def tryToBeController(id: Int): Either[ControllerExists, Unit] = {
    try {
      zookeeperScala.createEphemeralPath(controllerPath, id.toString)
      Right(())
    } catch {
      case _: ZkNodeExistsException =>
        val controllerId = zookeeperScala.readData[String](controllerPath)
        Left(ControllerExists(controllerId.toInt))
    }
  }

  def subscriberControllerChanges(onChange: Option[Int] => Unit): Unit =
    zookeeperScala.subscriberDataChanges[String](controllerPath) {
      case Some(value) => onChange(Some(value.toInt))
      case None        => onChange(None)
    }

  def registerSelf(): Unit = registerBroker(self)

  def registerBroker(broker: Broker): Unit =
    zookeeperScala.createEphemeralPath(getBrokerPath(broker.id), broker)

  def shutdown(): Unit = zookeeperScala.shutdown()
}

case object KafkaZookeeper {
  case class ControllerExists(controllerId: Int)
}
