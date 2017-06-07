package com.dematic.labs.dsp.drivers.kafka

import com.dematic.labs.dsp.configuration.DriverConfiguration
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.streaming.ProcessingTime

/** Setting the system property to define the configuration file:
  *
  * -Dconfig.file = path/to/file/application.conf
  */
object CumulativeCount {
  def main(args: Array[String]) {
    val config = new DriverConfiguration

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    val masterUrl = config.Spark.masterUrl
    if (!Strings.isNullOrEmpty(masterUrl)) builder.master(masterUrl)
    builder.appName(config.Driver.appName)
    val spark: SparkSession = builder.getOrCreate

    // create the input source, kafka
    val kafka = spark.readStream
      .format(config.Kafka.format())
      .option(config.Kafka.BootstrapServersKey, config.Kafka.bootstrapServers)
      .option(config.Kafka.subscribe, config.Kafka.topics)
      .option(config.Kafka.TopicSubscriptionKey, config.Kafka.startingOffsets)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalCount = kafka.groupBy("topic").count

    //2) write count to an output sink
    val query = totalCount.writeStream
      .format("console")
      .trigger(ProcessingTime(config.Spark.queryTrigger))
      .option(config.Spark.CheckpointLocationKey, config.Spark.checkpointLocation)
      .queryName("total_count")
      .outputMode(Complete)
      .start
    // 3) continue to run and wait for termination
    query.awaitTermination
  }
}
