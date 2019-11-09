package org.bilal.gossip.api

sealed trait Gossip

object Gossip{
  case class GossipRequest(from: Address, data: List[Address] = List.empty) extends Gossip
  case class GossipResponse(data: List[Address]) extends Gossip
}
