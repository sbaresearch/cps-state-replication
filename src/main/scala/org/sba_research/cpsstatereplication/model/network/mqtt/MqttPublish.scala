package org.sba_research.cpsstatereplication.model.network.mqtt

case class MqttPublish(`type`: Int, dup: Int, qos: Int, retain: Int, len: Int, topic: String, message: String) extends Mqtt

object MqttPublish {
  val `type`: Int = 3

  def apply(dup: Int, qos: Int, retain: Int, len: Int, topic: String, message: String): MqttPublish =
    MqttPublish(`type`, dup, qos, retain, len, topic, message)

}
