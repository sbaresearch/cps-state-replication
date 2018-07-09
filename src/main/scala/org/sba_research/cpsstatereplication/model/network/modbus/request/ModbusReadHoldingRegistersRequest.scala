package org.sba_research.cpsstatereplication.model.network.modbus.request

import org.sba_research.cpsstatereplication.model.network.modbus.{ModbusAdu, ModbusPdu, ModbusPduData}

object ModbusReadHoldingRegistersRequest {

  case class ModbusAduReadHoldingRegistersRequest(pdu: ModbusPduReadHoldingRegistersRequest) extends ModbusAdu

  case class ModbusPduReadHoldingRegistersRequest(
                                                   functionCode: Byte,
                                                   data: Option[ModbusPduDataReadHoldingRegistersRequest]
                                                 ) extends ModbusPdu

  case class ModbusPduDataReadHoldingRegistersRequest(
                                                       startingAddress: Seq[Byte],
                                                       quantityOfRegisters: Seq[Byte]
                                                     ) extends ModbusPduData


  val functionCode: Byte = 0x03

  def apply(
             startingAddress: Seq[Byte],
             quantityOfRegisters: Seq[Byte]
           ): ModbusAduReadHoldingRegistersRequest = {
    ModbusAduReadHoldingRegistersRequest(
      pdu = ModbusPduReadHoldingRegistersRequest(
        functionCode = functionCode,
        data = Option(
          ModbusPduDataReadHoldingRegistersRequest(
            startingAddress,
            quantityOfRegisters
          )
        )
      )
    )
  }
}
