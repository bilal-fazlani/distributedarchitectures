package org.dist.kvstore

import java.net.InetAddress

object InetAddressAndPort {
  //FIXME: Remove this.
  def create(hostIp:String, port:Int) = {
    new InetAddressAndPort(InetAddress.getByName(hostIp), port)
  }
}

case class InetAddressAndPort(address: InetAddress, port: Int) {
  val defaultPort: Int = 7000
}

