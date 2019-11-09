package org.bilal.gossip

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.bilal.gossip.api.Address
import org.bilal.gossip.api.Gossip.{GossipRequest, GossipResponse}
import org.bilal.gossip.codec.Codecs
import org.bilal.remote.{TcpClient, TcpServer}

import scala.language.implicitConversions
import scala.util.Random

class Gossiper(self: Address,
               seed: Address,
               scheduledExecutorService: ScheduledExecutorService)
    extends Thread
    with Codecs {

  private val handler: GossipRequest => GossipResponse = req => {
    digest = (digest :+ req.from).distinct
    digest = (digest :++ req.data).distinct
    GossipResponse(digest.distinct)
  }
  private val server =
    new TcpServer[GossipRequest, GossipResponse](handler, self.port)
  var digest = List[Address](seed)

  override def start(): Unit = {
    println(s"starting server at ${self.port}")
    server.start()
    println(s"gossiper server started at ${self.port}")
    scheduleNextSync
    println("scheduled sync")
  }

  implicit def runnable(f: => Unit): Runnable = () => f

  private def println(text: String): Unit =
    Predef.println(s"[${self.port}]: $text")

  private def scheduleNextSync: ScheduledFuture[_] =
    scheduledExecutorService.schedule(
      syncWithRandomTarget(),
      3,
      TimeUnit.SECONDS
    )

  private def syncWithRandomTarget(): Unit = {
    def total: Int = digest.size
    println(s"data total: $total")

    if (total == 1 && digest.head == self) {
      println("only self is present. skipping sync")
    } else if (total > 0) {
      val randomTarget = digest(Random.nextInt(total))
      println(s"random target: $randomTarget")
      syncData(randomTarget)
      println(s"sync finished. new total: $total")
    } else {
      println("no data is present. skipping sync")
    }
    scheduleNextSync
    println("next sync scheduled")
  }

  private def syncData(target: Address): Unit = {
    if (target != self) {
      val response = TcpClient
        .sendReceiveTcp[GossipRequest, GossipResponse](
          GossipRequest(self),
          target.toTuple
        )
      digest = (digest ++ response.data).distinct
    } else {
      println("self was selected. skipping sync")
    }
  }
}
