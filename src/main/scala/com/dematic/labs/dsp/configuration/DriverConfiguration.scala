package com.dematic.labs.dsp.configuration

import com.typesafe.config._

class DriverConfiguration {
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

    lazy val masterUrl: String = config.getString(MasterKey)
    lazy val checkpointLocation: String = config.getString(CheckpointLocationKey)
    lazy val sqlShufflePartition: String = config.getString(SqlShufflePartitionKey)
    lazy val outputMode: String = config.getString(OutputModeKey)
    lazy val watermarkTime: String = config.getString(WatermarkTimeKey)
    lazy val queryTrigger: String = config.getString(QueryTriggerKey)
  }

  object Kafka {
    val TopicSubscriptionKey = "kafka.subscribe"
    val BootstrapServersKey = "kafka.bootstrap.servers"
    val TopicsKey = "kafka.topics"
    val StartingOffsetsKey = "kafka.startingOffsets"
    val KeySerializerKey = "kafka.key.serializer"
    val ValueSerializeKey = "kafka.value.serializer"
    val AcksKey = "kafka.acks"
    val ProducerTypeKey = "kafka.producer.type"
    val RetriesKey = "kafka.retries"

    lazy val bootstrapServers: String = config.getString(BootstrapServersKey)
    lazy val topics: String = config.getString(TopicsKey)
    lazy val startingOffsets: String = config.getString(StartingOffsetsKey)
    lazy val keySerializer: String = config.getString(KeySerializerKey)
    lazy val valueSerializer: String = config.getString(ValueSerializeKey)
    lazy val acks: String = config.getString(AcksKey)
    lazy val producerType: String = config.getString(ProducerTypeKey)
    lazy val retries: String = config.getString(RetriesKey)

    def format() = "kafka"
  }

  object Cassandra {
    val UsernameKey = "cassandra.username"
    val PasswordKey = "cassandra.password"

    lazy val username: String = config.getString(UsernameKey)
    lazy val password: String = config.getString(PasswordKey)
  }
}