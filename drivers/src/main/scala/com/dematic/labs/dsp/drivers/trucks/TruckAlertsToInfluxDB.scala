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
  * Puts temp messages from kakfa topic to influxdb
  */
object TruckAlertsToInfluxDB {
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

      // define the truck json schema
      val schema: StructType = new StructType().add("alert_time", new StructType().add("start", TimestampType)
        .add("end", TimestampType))
        .add("truck",StringType)
        .add("alerts",LongType)

      import sparkSession.implicits._

      /*
       val alerts = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema) as "alerts").
        select("alerts.*")
      val outputAlerts= alerts.select("truck","alerts","alert_time.start","alert_time.end")
*/

      // get kafka date as we can at least get the influx db write tested.
      val alerts = kafka.selectExpr("cast (value as string) as json", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)].
        select(from_json($"json", schema) as "alerts", $"timestamp").
        select("alerts.*","timestamp")
      val outputAlerts= alerts.select("truck","alerts","timestamp","alert_time.start","alert_time.end")

      outputAlerts.writeStream
        .format("console").queryName("truckAlertTableToConsole")
        .outputMode("update")
        .start

      lazy val influxDBSink = new InfluxDBAlertSink(config)

      outputAlerts.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("influxDBTruckAlerts")
        .foreach(influxDBSink)
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}

