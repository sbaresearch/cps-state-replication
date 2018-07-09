package org.sba_research.cpsstatereplication.model.network.modbus

import org.sba_research.cpsstatereplication.model.network.Layer

trait ModbusAdu extends Layer {

  // val additionalAddress = ???
  val pdu: ModbusPdu
  // val errorCheck = ???

}
