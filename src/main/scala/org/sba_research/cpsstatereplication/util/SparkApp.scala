package org.sba_research.cpsstatereplication.util

import org.apache.spark.{SparkContext, SparkConf}

class SparkApp extends SparkConfig {
  override val sparkConf = new SparkConf()
  sparkConfig.foreach { case (k, v) => sparkConf.setIfMissing(k, v) }
  val sparkContext = new SparkContext(sparkConf)
}
