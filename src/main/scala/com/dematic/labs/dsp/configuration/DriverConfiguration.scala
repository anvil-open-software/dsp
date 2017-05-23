package com.dematic.labs.dsp.configuration

import com.typesafe.config._

class DriverConfiguration {
  private val config =  ConfigFactory.load()

  object driver {
     lazy val appName: String = config.getString("driver.appName")
     lazy val masterUrl: String = config.getString("driver.masterUrl")
  }

  object spark {
    lazy val checkpointDir: String = config.getString("spark.checkpoint.dir")
    lazy val sqlShufflePartition: String = config.getString("sql.shuffle.partition")
    lazy val outputMode: String = config.getString("output.mode")
    lazy val watermarkTime: String = config.getString("watermark.time")
    lazy val queryTrigger: String = config.getString("query.trigger")
  }
}