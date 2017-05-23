package com.dematic.labs.dsp.drivers.kafka

import com.dematic.labs.dsp.configuration.DriverConfiguration
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession

object CumulativeCount {
  def main(args: Array[String]): Unit = {
    // application specific configuration is set using system property, ex: -Dconfig.file=pathTo/application.conf
    val config = new DriverConfiguration

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    val masterUrl = config.Driver.masterUrl
    if (!Strings.isNullOrEmpty(masterUrl)) builder.master(masterUrl)
    builder.appName(config.Driver.appName)
    builder.config(config.Spark.CheckpointDirProperty, config.Spark.checkpointDir)
    val spark: SparkSession = builder.getOrCreate
  }
}
