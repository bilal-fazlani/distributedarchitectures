package org.bilal.simplekafka2

class Server2(kafkaClient: KafkaZookeeper, controller: Controller2) {
  def start(): Unit = {
    kafkaClient.registerSelf()
    controller.start()
  }
  def shutdown(): Unit = {
    kafkaClient.shutdown()
  }
}
