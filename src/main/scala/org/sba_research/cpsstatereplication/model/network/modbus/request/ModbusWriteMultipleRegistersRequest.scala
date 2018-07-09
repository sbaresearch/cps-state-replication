package org.sba_research.cpsstatereplication.model.network.modbus.request

import org.sba_research.cpsstatereplication.model.network.modbus.{ModbusAdu, ModbusPdu, ModbusPduData}

object ModbusWriteMultipleRegistersRequest {

  case class ModbusAduWriteMultipleRegistersRequest(pdu: ModbusPduWriteMultipleRegistersRequest) extends ModbusAdu

  case class ModbusPduWriteMultipleRegistersRequest(
                                                     functionCode: Byte,
                                                     data: Option[ModbusPduDataWriteMultipleRegistersRequest]
                                                   ) extends ModbusPdu

  case class ModbusPduDataWriteMultipleRegistersRequest(
                                                         startingAddress: Seq[Byte],
                                                         quantityOfRegisters: Seq[Byte],
                                                         byteCount: Byte,
                                                         registersValue: Seq[Byte]
                                                       ) extends ModbusPduData

  val functionCode: Byte = 0x10

  val defaultRegistersValue: Seq[Byte] = Seq(0x00, 0x00)

  def apply(
             startingAddress: Seq[Byte],
             quantityOfRegisters: Seq[Byte],
             registersValue: Seq[Byte] = defaultRegistersValue
           ): ModbusAduWriteMultipleRegistersRequest =
    ModbusAduWriteMultipleRegistersRequest(
      pdu = ModbusPduWriteMultipleRegistersRequest(
        functionCode = functionCode,
        data = Option(
          ModbusPduDataWriteMultipleRegistersRequest(
            startingAddress,
            quantityOfRegisters,
            BigInt(quantityOfRegisters.toArray).*(2).toByte,
            registersValue
          )
        )
      )
    )

}
