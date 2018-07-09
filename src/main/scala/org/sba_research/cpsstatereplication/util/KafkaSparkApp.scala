package org.sba_research.cpsstatereplication.util

import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

class KafkaSparkApp extends SparkApp {
  val kafkaConsumerConfig: Map[String, String] = appConfig.as[Map[String, String]]("kafka.consumer")
  val kafkaProducerConfig: Map[String, String] = appConfig.as[Map[String, String]]("kafka.producer")
  val streamingBatchDuration: FiniteDuration = appConfig.as[FiniteDuration]("spark.streaming.batchDuration")
}
