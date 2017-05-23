package com.dematic.labs.dsp.configuration

import com.typesafe.config._

class DriverConfiguration {
  private val config = ConfigFactory.load()

  object Driver {
    val AppNameKey = "driver.appName"
    val MasterUrlKey = "driver.masterUrl"

    lazy val appName: String = config.getString(AppNameKey)
    lazy val masterUrl: String = config.getString(MasterUrlKey)
  }

  object Spark {
    val CheckpointLocationKey = "spark.sql.streaming.checkpointLocation"
    val SqlShufflePartitionKey = "sql.shuffle.partition"
    val OutputModeKey = "output.mode"
    val WatermarkTimeKey = "watermark.time"
    val QueryTriggerKey = "query.trigger"

    lazy val checkpointLocation: String = config.getString(CheckpointLocationKey)
    lazy val sqlShufflePartition: String = config.getString(SqlShufflePartitionKey)
    lazy val outputMode: String = config.getString(OutputModeKey)
    lazy val watermarkTime: String = config.getString(WatermarkTimeKey)
    lazy val queryTrigger: String = config.getString(QueryTriggerKey)
  }

  object Kafka {
    val TopicSubscriptionKey = "subscribe"
    val BootstrapServersKey = "kafka.bootstrap.servers"
    val TopicsKey = "kafka.topics"
    val StartingOffsetsKey = "startingOffsets"

    lazy val bootstrapServers: String = config.getString(BootstrapServersKey)
    lazy val topics: String = config.getString(TopicsKey)
    lazy val startingOffsets: String = config.getString(StartingOffsetsKey)

    def format() = "kafka"
  }

  object Cassandra {
    val UsernameKey = "cassandra.username"
    val PasswordKey = "cassandra.password"

    lazy val username: String = config.getString(UsernameKey)
    lazy val password: String = config.getString(PasswordKey)
  }
}