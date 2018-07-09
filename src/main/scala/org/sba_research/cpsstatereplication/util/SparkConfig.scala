package org.sba_research.cpsstatereplication.util

import org.apache.spark.SparkConf
import net.ceedubs.ficus.Ficus._

trait SparkConfig extends AppConfig {
  val sparkConf: SparkConf
  val sparkConfig: Map[String, String] = appConfig.as[Map[String, String]]("spark.base")
}
