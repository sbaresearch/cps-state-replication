package org.sba_research.cpsstatereplication.util

import com.typesafe.config.{Config, ConfigFactory}

trait AppConfig {
  private val baseConfig: Config = ConfigFactory.load()
  val appConfig: Config = baseConfig.getConfig("cpsReplay")
}
