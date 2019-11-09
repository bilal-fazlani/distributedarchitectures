package org.bilal.gossip.api

case class Address(host: String, port: Int) {
  def toTuple: (String, Int) = (host, port)
}