package org.dist.simplekafka2

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka2.KafkaClient2.PartitionReplicas

class KafkaClient2(zookeeperClient: ZookeeperClient2) {
  def registerBroker(broker: Broker): Unit = {
    val path = s"/brokers/ids/${broker.id}"
    zookeeperClient.createEphemeralPath(path, broker)
  }

  def allBrokerIds(): Set[Int] = zookeeperClient
    .allChildren("/brokers/ids")

  def createTopic(name:String, noOfPartitions:Int, replicationFactor:Int):Unit = {
    val allBrokers = allBrokerIds()
    val partitionReplicas: Set[PartitionReplicas] = createPartitionReplicasForBrokers(allBrokers, noOfPartitions, replicationFactor)
    zookeeperClient.createPersistantPath(s"/brokers/topics/$name", partitionReplicas)
  }

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
object KafkaClient2{
  case class PartitionReplicas(partitionId:Int, brokerIds:List[Int])
  case class PartitionBroker(partitionId:Int, brokerId: Int)
}
