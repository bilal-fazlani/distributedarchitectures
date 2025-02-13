package org.bilal.simplekafka2.extentions

import org.dist.queue.utils.ZkUtils.Broker

object Extensions {
  implicit class RichBroker(broker: Broker) {
    def targetAddress: (String, Int) = (broker.host, broker.port)
  }
}
