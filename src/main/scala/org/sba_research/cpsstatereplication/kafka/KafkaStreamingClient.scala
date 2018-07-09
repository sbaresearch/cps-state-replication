package org.sba_research.cpsstatereplication.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaStreamingClient {

  def createStream[K, V](
                          streamingContext: StreamingContext,
                          consumerConfig: Map[String, String],
                          topics: Array[String]
                        ): InputDStream[ConsumerRecord[K, V]] = {
    KafkaUtils.createDirectStream[K, V](
      streamingContext,
      PreferConsistent,
      Subscribe[K, V](topics, consumerConfig)
    )
  }

  def createProducer(sparkContext: SparkContext, producerConfig: Map[String, String]): Broadcast[KafkaSink] =
    sparkContext.broadcast(KafkaSink(producerConfig))

}
