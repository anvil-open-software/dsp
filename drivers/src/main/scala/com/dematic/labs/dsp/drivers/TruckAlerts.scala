package com.dematic.labs.dsp.drivers

import java.sql.Timestamp

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.data.Utils.toTimeStamp
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
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
        StructField("_timestamp", StringType, nullable = false),
        StructField("channel", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("unit", StringType, nullable = false)
      ))

      import sparkSession.implicits._

      // convert to json and select only channel 'T_motTemp_Lft'
      val channels = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema).as("channels")).
        select("channels.*").
        where("channel == 'T_motTemp_Lft'")

      // group by 1 hour and truck and find min/max and trigger an alert if condition is meet
      val alerts = channels.groupBy(window(channels.col("_timestamp"), "1 hours").
        as(Symbol("alert_time")), channels.col("truck")).
        agg(min('value) as "min", max('value) as "max", collect_list('_timestamp) as "times", collect_list('value) as "values").
        where(col("max") - col("min") > 10)

      //todo: think about watermark

      alerts.writeStream
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("truckAlerts")
        .outputMode("complete")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            println(row.toString())
          }

          override def close(errorOrNull: Throwable) {}

        }).start


    /*  // just write to the console
      alerts.writeStream
        .format("console")
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("truckAlerts")
        .outputMode("complete")
        .start*/

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}
