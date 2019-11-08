package org.bilal.simplekafka2

class Server2(val kafkaZookeeper: KafkaZookeeper, val controller: Controller2) {
  def start(): Unit = {
    kafkaZookeeper.registerSelf()
    controller.start()
  }
  def shutdown(): Unit = {
    kafkaZookeeper.shutdown()
  }
}
