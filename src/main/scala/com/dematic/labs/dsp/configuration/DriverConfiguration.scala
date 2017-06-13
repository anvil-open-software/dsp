package com.dematic.labs.dsp.configuration

import com.typesafe.config._

object DriverConfiguration {
  private val config = ConfigFactory.load()

  object Driver {
    val AppNameKey = "driver.appName"
    lazy val appName: String = config.getString(AppNameKey)
  }

  object Spark {
    val MasterKey = "spark.master"
    val CheckpointLocationKey = "spark.sql.streaming.checkpointLocation"
    val SqlShufflePartitionKey = "spark.sql.shuffle.partition"
    val OutputModeKey = "spark.output.mode"
    val WatermarkTimeKey = "spark.watermark.time"
    val QueryTriggerKey = "spark.query.trigger"
    val CassandraHostKey = "spark.cassandra.connection.host"

    lazy val masterUrl: String = config.getString(MasterKey)
    lazy val checkpointLocation: String = config.getString(CheckpointLocationKey)
    lazy val sqlShufflePartition: String = config.getString(SqlShufflePartitionKey)
    lazy val outputMode: String = config.getString(OutputModeKey)
    lazy val watermarkTime: String = config.getString(WatermarkTimeKey)
    lazy val queryTrigger: String = config.getString(QueryTriggerKey)
    lazy val cassandraHost: String = config.getString(CassandraHostKey)
  }

  object Kafka {
    val BootstrapServersKey = "kafka.bootstrap.servers"
    val TopicsKey = "kafka.topics"
    val TopicSubscriptionKey = "kafka.subscribe"
    val StartingOffsetsKey = "kafka.startingOffsets"

    lazy val bootstrapServers: String = config.getString(BootstrapServersKey)
    lazy val topics: String = config.getString(TopicsKey)
    lazy val subscribe: String = config.getString(TopicSubscriptionKey)
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