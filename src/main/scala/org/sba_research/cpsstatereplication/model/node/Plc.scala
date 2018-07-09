package org.sba_research.cpsstatereplication.model.node

case class Plc(name: String, inet: Inet, macAddress: String, program: Program) extends ProgramNode
