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
    val CassandraUsernameKey = "spark.cassandra.auth.username"
    val CassandraPasswordKey = "spark.cassandra.auth.password"

    lazy val masterUrl: String = config.getString(MasterKey)
    lazy val checkpointLocation: String = config.getString(CheckpointLocationKey)
    lazy val sqlShufflePartition: String = config.getString(SqlShufflePartitionKey)
    lazy val outputMode: String = config.getString(OutputModeKey)
    lazy val watermarkTime: String = config.getString(WatermarkTimeKey)
    lazy val queryTrigger: String = config.getString(QueryTriggerKey)
    lazy val cassandraHost: String = config.getString(CassandraHostKey)
    lazy val cassandraUsername: String = config.getString(CassandraUsernameKey)
    lazy val cassandraPassword: String = config.getString(CassandraPasswordKey)
  }

  object Kafka {
    val BootstrapServersKey = "kafka.bootstrap.servers"
    val TopicsKey = "kafka.topics"
    val TopicSubscriptionKey = "kafka.subscribe"
    val StartingOffsetsKey = "kafka.startingOffsets"
    val MaxOffsetsPerTriggerKey = "kafka.maxOffsetsPerTrigger"

    lazy val bootstrapServers: String = config.getString(BootstrapServersKey)
    lazy val topics: String = config.getString(TopicsKey)
    lazy val subscribe: String = config.getString(TopicSubscriptionKey)
    lazy val startingOffsets: String = config.getString(StartingOffsetsKey)
    lazy val maxOffsetsPerTrigger: String = config.getString(MaxOffsetsPerTriggerKey)

    def format() = "kafka"
  }

  object Cassandra {
    val KeyspaceKey = "cassandra.keyspace"

    lazy val keyspace: String = config.getString(KeyspaceKey)
  }

  def removeQualifier(key: String): String = {
    if (key.startsWith("driver")) {
      key stripPrefix "driver."
    } else if (key.startsWith("spark")) {
      key stripPrefix "spark."
    } else if (key.startsWith("kafka")) {
      key stripPrefix "kafka."
    } else if (key.startsWith("cassandra")) {
      key stripPrefix "cassandra."
    } else {
      key
    }
  }
}