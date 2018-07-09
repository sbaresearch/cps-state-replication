package org.sba_research.cpsstatereplication.model.network.mqtt

import org.sba_research.cpsstatereplication.model.network.Layer

trait Mqtt extends Layer {
  val `type`: Int
  val dup: Int
  val qos: Int
  val retain: Int
  val len: Int
}
