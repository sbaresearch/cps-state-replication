package org.sba_research.cpsstatereplication.model.network.modbus

trait ModbusPdu {

  val functionCode: Byte
  val data: Option[ModbusPduData]

}
