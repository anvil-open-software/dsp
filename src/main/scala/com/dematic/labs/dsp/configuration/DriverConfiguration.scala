package com.dematic.labs.dsp.configuration

import com.typesafe.config._

class DriverConfiguration {
  private val config = ConfigFactory.load()

  object Driver {
    val AppNameProperty = "driver.appName"
    val MasterUrlProperty = "driver.masterUrl"

    lazy val appName: String = config.getString(AppNameProperty)
    lazy val masterUrl: String = config.getString(MasterUrlProperty)
  }

  object Spark {
    val CheckpointDirProperty = "spark.checkpoint.dir"
    val SqlShufflePartitionProperty = "sql.shuffle.partition"
    val OutputModeProperty = "output.mode"
    val WatermarkTimeProperty = "watermark.time"
    val QueryTriggerProperty = "query.trigger"

    lazy val checkpointDir: String = config.getString(CheckpointDirProperty)
    lazy val sqlShufflePartition: String = config.getString(SqlShufflePartitionProperty)
    lazy val outputMode: String = config.getString(OutputModeProperty)
    lazy val watermarkTime: String = config.getString(WatermarkTimeProperty)
    lazy val queryTrigger: String = config.getString(QueryTriggerProperty)
  }

  object Kafka {
    val BootstrapServersProperty = "kafka.bootstrapServers"
    val TopicsProperty = "kafka.topics"


    lazy val bootstrapServers: String = config.getString(BootstrapServersProperty)
    lazy val topics: String = config.getString(TopicsProperty)
  }

  object Cassandra {
    val UsernameProperty = "cassandra.username"
    val PasswordProperty = "cassandra.password"

    lazy val username: String = config.getString(UsernameProperty)
    lazy val password: String = config.getString(PasswordProperty)
  }
}