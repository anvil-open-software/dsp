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

    // create the input source, kafka
    val kafka = spark.readStream
      .format(config.Kafka.format())
      .option(config.Kafka.BootstrapServersProperty, config.Kafka.bootstrapServers)
      .option(config.Kafka.TopicsProperty, config.Kafka.topics)
      .option(config.Kafka.TopicSubscriptionProperty, config.Kafka.startingOffsets)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalCount = kafka.groupBy("topic").count
  }
}
