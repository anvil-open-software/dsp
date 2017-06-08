package com.dematic.labs.dsp.drivers.kafka

import com.dematic.labs.dsp.configuration.DriverConfiguration._
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.streaming.ProcessingTime

/** Setting the system property to define the configuration file:
  *
  * -Dconfig.file=path/to/file/application.conf
  */
object CumulativeCount {
  def main(args: Array[String]) {
    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(Spark.masterUrl)) builder.master(Spark.masterUrl)
    builder.appName(Driver.appName)
    val spark: SparkSession = builder.getOrCreate

    // create the input source, kafka
    val kafka = spark.readStream
      .format(Kafka.format())
      .option(Kafka.BootstrapServersKey, Kafka.bootstrapServers)
      .option(Kafka.subscribe, Kafka.topics)
      .option(Kafka.TopicSubscriptionKey, Kafka.startingOffsets)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalCount = kafka.groupBy("topic").count

    //2) write count to an output sink
    val query = totalCount.writeStream
      .format("console")
      .trigger(ProcessingTime(Spark.queryTrigger))
      .option(Spark.CheckpointLocationKey, Spark.checkpointLocation)
      .queryName("total_count")
      .outputMode(Complete)
      .start
    // 3) continue to run and wait for termination
    query.awaitTermination
  }
}
