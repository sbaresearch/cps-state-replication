package org.sba_research.cpsstatereplication.model.node

import java.net.InetAddress

case class Inet(ip: InetAddress, netmask: InetAddress)

object Inet {
  def apply(ip: String, netmask: String): Inet = new Inet(InetAddress.getByName(ip), InetAddress.getByName(netmask))
}
