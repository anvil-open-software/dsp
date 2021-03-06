/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers.trucks

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.dematic.labs.dsp.drivers.functions.TemperatureAnomalyCount
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types._

object TruckAlerts {
  // should only be  used with testing
  private var injectedDriverConfiguration: DriverConfiguration = _

  private[drivers] def setDriverConfiguration(driverConfiguration: DriverConfiguration) {
    injectedDriverConfiguration = driverConfiguration
  }

  def main(args: Array[String]) {
    // driver configuration
    val config = if (injectedDriverConfiguration == null) {
      new DefaultDriverConfiguration.Builder().build
    } else {
      injectedDriverConfiguration
    }

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(config.getSparkMaster)) builder.master(config.getSparkMaster)
    builder.appName(config.getDriverAppName)
    val sparkSession: SparkSession = builder.getOrCreate

    // hook up Prometheus listener for monitoring
    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      sparkSession.streams.addListener(new PrometheusStreamingQueryListener(sparkSession.sparkContext.getConf,
        config.getDriverAppName))
    }

    val temperatureAnomalyCount = new TemperatureAnomalyCount(config.getDriverAlertThreshold)

    // create the kafka input source
    try {
      val kafka = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option(config.getKafkaSubscribe, config.getKafkaTopics)
        .option("startingOffsets", config.getKafkaStartingOffsets)
        .load

      // define the truck json schema
      val schema: StructType = StructType(Seq(
        StructField("truck", StringType, nullable = false),
        StructField("_timestamp", TimestampType, nullable = false),
        StructField("channel", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("unit", StringType, nullable = false)
      ))

      import sparkSession.implicits._

      // convert to json and select only channel 'T_motTemp_Lft'
      val channels = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema) as "channels").
        select("channels.*").
        where("channel == 'T_motTemp_Lft'")

      // group by 1 hour and truck and find min/max and trigger an alert if condition is meet
      val alerts = channels.
        withWatermark("_timestamp", config.getSparkWatermarkTime). // time to keep data around
        groupBy(window('_timestamp, config.getSparkWindowDuration, config.getSparkWindowSlideDuration) as "alert_time", 'truck).
        agg(temperatureAnomalyCount('_timestamp, 'value) as "alerts", sort_array(collect_list(struct('_timestamp, 'value))) as "values").
        withColumn("processing_time", current_timestamp()).
        where('alerts > 0)

      // write results to kafka
      alerts.selectExpr("to_json(struct(processing_time, truck, alert_time, alerts, values)) AS value").
        writeStream
        .format("kafka")
        .queryName("truckAlerts")
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option("topic", config.getKafkaOutputTopics)
        .option("checkpointLocation", config.getSparkCheckpointLocation)
        .outputMode(config.getSparkOutputMode)
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}