package org.dist.simplekafka2

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas

class KafkaClient2(zookeeperClient: ZookeeperClient2) {
  private val brokerIdsPath = "/brokers/ids"
  private val topicsPath = "/brokers/topics"

  private def getTopicPath(name: String): String = s"$topicsPath/$name"
  private def getBrokerPath(id: Int): String = s"$brokerIdsPath/$id"

  def registerBroker(broker: Broker): Unit = {
    zookeeperClient.createEphemeralPath(getBrokerPath(broker.id), broker)
  }

  def allBrokerIds(): Set[Int] = zookeeperClient
    .allChildren(brokerIdsPath)


  def allBrokers(): Set[Broker] = zookeeperClient
    .allChildren(brokerIdsPath)
    .map(getBrokerInfo)

  def createTopic(name:String, noOfPartitions:Int, replicationFactor:Int):Unit = {
    val allBrokers = allBrokerIds()
    val partitionReplicas: Set[PartitionReplicas] = createPartitionReplicasForBrokers(allBrokers, noOfPartitions, replicationFactor)
    zookeeperClient.createPersistantPath(getTopicPath(name), partitionReplicas)
  }

  def getBrokerInfo(id:Int): Broker = zookeeperClient.readData[Broker](getBrokerPath(id))

  def getPartitionAssignmentsForTopic(name:String):Set[PartitionReplicas] = zookeeperClient.readData(getTopicPath(name))

  def subscribeToTopicChanges(handler: List[String] => Unit): List[String] =
      zookeeperClient.subscribeChildChanges(topicsPath)(handler)

  def subscribeToBrokerChanges(handler: List[Int] => Unit): List[String] =
    zookeeperClient.subscribeChildChanges(brokerIdsPath)(x => handler(x.map(_.toInt)))

  private def createPartitionReplicasForBrokers(brokers: Set[Int], noOfPartitions:Int, replicationFactor:Int):Set[PartitionReplicas] = {
    val numberOfBrokers: Int = brokers.size

    var partitionReplicas:Set[PartitionReplicas] = Set.empty

    var brokerId:Int = 1

    (1 to noOfPartitions).foreach(p => {
      var brokerList = List[Int]()
      (1 to replicationFactor).foreach{ _:Int =>
        brokerList = brokerList  :+ brokerId
          if(brokerId == numberOfBrokers)
            brokerId = 1
          else
            brokerId += 1
      }
      partitionReplicas += PartitionReplicas(p, brokerList)
    })
    partitionReplicas
  }
}
