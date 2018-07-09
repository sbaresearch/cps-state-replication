package org.sba_research.cpsstatereplication.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * This class represents the Kafka producer that can be used to send messages.
  *
  * Cf. https://allegro.tech/2015/08/spark-kafka-integration.html
  *
  * @param createProducer a method that returns a Kafka producer
  */
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))

}

object KafkaSink {
  def apply(config: Map[String, Object]): KafkaSink = {
    val f = () => {

      import scala.collection.JavaConversions._

      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
