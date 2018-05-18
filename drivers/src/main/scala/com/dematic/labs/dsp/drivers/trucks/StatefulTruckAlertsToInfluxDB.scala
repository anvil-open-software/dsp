package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.types._

/**
  * Simple data pusher, puts all truck topic alerts from kakfa to influxdb.
  *
  * Any changes to alert topic schema may potentially require change here.
  *
  */
object StatefulTruckAlertsToInfluxDB {
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

    // note influx db connector is not serializable and we must have only one influxDB per jvm, executor

    // create the kafka input source
    try {
      val kafka = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option(config.getKafkaSubscribe, config.getKafkaTopics)
        .option("startingOffsets", config.getKafkaStartingOffsets)
        .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger)
        .load

      // define the alert json schema which has been flattened out below


      val schema: StructType = new StructType()
        .add("truck",StringType)
        .add("min", new StructType().add("_timestamp", TimestampType).add("value", DoubleType))
        .add("max", new StructType().add("_timestamp", TimestampType).add("value", DoubleType))

      import sparkSession.implicits._


      // use kafka write date for now instead of digging up the max of the "values" subtable
      val alerts = kafka.selectExpr("cast (value as string) as json", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)].
        select(from_json($"json", schema) as "alerts", $"timestamp").
        select("alerts.*","timestamp")
      val outputAlerts= alerts.select("truck","timestamp","min","max","count()")
      lazy val influxDBSink = new InfluxDBStatefulAlertSink(config)

      outputAlerts.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("statefulAlertsToInfluxDB")
        .foreach(influxDBSink)
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}

