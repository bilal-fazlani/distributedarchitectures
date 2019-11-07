package org.bilal.simplekafka2

class Server2(kafkaClient: KafkaClient2, controller: Controller2) {
  def start(): Unit = {
    kafkaClient.registerSelf()
    controller.startup()
  }
  def shutdown(): Unit = {
    kafkaClient.shutdown()
  }
}
