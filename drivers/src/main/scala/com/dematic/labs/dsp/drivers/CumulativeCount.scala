/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.DefaultDriverConfiguration
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.streaming.Trigger._

/** Setting the system property to define the configuration file:
  *
  * -Dconfig.file=path/to/file/application.conf
  */
object CumulativeCount {
  def main(args: Array[String]) {
    // driver configuration
    val config = new DefaultDriverConfiguration.Builder().build

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(config.getSparkMaster)) builder.master(config.getSparkMaster)
    builder.appName(config.getDriverAppName)
    val spark: SparkSession = builder.getOrCreate

    // hook up Prometheus listener for monitoring
    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      spark.streams.addListener(new PrometheusStreamingQueryListener(spark.sparkContext.getConf, config.getDriverAppName))
    }

    // create the input source, kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
      .option(config.getKafkaSubscribe, config.getKafkaTopics)
      .option("startingOffsets", config.getKafkaStartingOffsets)
      .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalCount = kafka.groupBy("topic").count

    //2) write count to an output sink
    val query = totalCount.writeStream
      .format("console")
      .trigger(ProcessingTime(config.getSparkQueryTrigger))
      .option("checkpointLocation", config.getSparkCheckpointLocation)
      .queryName("total_count")
      .outputMode(Complete)
      .start
    // 3) continue to run and wait for termination
    query.awaitTermination
  }
}
