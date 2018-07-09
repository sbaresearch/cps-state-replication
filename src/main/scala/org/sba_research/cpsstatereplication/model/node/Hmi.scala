package org.sba_research.cpsstatereplication.model.node

case class Hmi(name: String, inet: Inet, macAddress: String, program: Program) extends ProgramNode
