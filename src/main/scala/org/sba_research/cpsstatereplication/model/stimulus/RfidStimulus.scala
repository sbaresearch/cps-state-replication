package org.sba_research.cpsstatereplication.model.stimulus

case class RfidStimulus(timestamp: String, twinName: String, value: String) extends Stimulus
