package org.sba_research.cpsstatereplication.model.stimulus

import org.sba_research.cpsstatereplication.model.link.NetworkStimulusLink
import org.sba_research.cpsstatereplication.model.log.SystemLog
import org.sba_research.cpsstatereplication.model.network.modbus.ModbusAdu

case class AmlStimuli(
                        network: Map[ModbusAdu, Seq[NetworkStimulusLink]],
                        log: Set[SystemLog]
                      )
