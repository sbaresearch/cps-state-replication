package org.sba_research.cpsstatereplication.model.log

trait SystemLog {
  val name: String
  val timestamp: String
  val value: String
}
