package org.bilal.gossip

import java.util.concurrent.Executors

import org.bilal.gossip.api.Address
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.time.SpanSugar.convertLongToGrainOfTime
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

class GossipTest
    extends FunSuite
    with Matchers
    with MockitoSugar
    with Eventually {

  private implicit def toSpan(duration: FiniteDuration): Span = Span(duration.toSeconds, Seconds)

  test("should start gossip") {
    val executor = Executors.newSingleThreadScheduledExecutor()
    val seedAddress = Address("localhost", 6000)

    val gossiper1 = new Gossiper(seedAddress, seedAddress, executor)
    gossiper1.start()

    val gossiper2 =
      new Gossiper(Address("localhost", 7000), seedAddress, executor)
    gossiper2.start()

    val gossiper3 =
      new Gossiper(Address("localhost", 8000), seedAddress, executor)
    gossiper3.start()

    eventually(Timeout(10.seconds), Interval(1.seconds)) {
      gossiper1.digest should have size 3
      gossiper2.digest should have size 3
      gossiper3.digest should have size 3
    }
  }
}
