package org.sba_research.cpsstatereplication.model.network.modbus

import org.sba_research.cpsstatereplication.model.network.Layer

case class MbTcp(transId: Int, protId: Int, len: Int, unitId: Int) extends Layer

object MbTcp {

  val port = 502

}
