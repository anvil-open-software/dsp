package com.dematic.labs.dsp.drivers.trucks

import java.util.concurrent.TimeUnit

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.dematic.labs.dsp.tsdb.influxdb.InfluxDBConnector
import com.google.common.base.Strings
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger._
import org.influxdb.dto.Point
import org.slf4j.{Logger, LoggerFactory}

/**
  * Puts temp messages from kakfa topic to influxdb
  */
object TruckTopicToInfluxDB {
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

    // note influx db connector is not serializable and lazy val declaration ensures one per jvm, executor
    lazy val influxDB = InfluxDBConnector.initializeConnection(config)

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
      val schema: StructType = StructType(Seq(
        StructField("truck", StringType, nullable = false),
        StructField("_timestamp", LongType, nullable = false),
        StructField("channel", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false)
      ))

      import sparkSession.implicits._

      // convert to json and select only channel 'T_motTemp_Lft'
      val channels = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema) as "channels").
        select("channels.*").
        where("channel == 'T_motTemp_Lft'")

      channels.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("truckAlerts")
        .foreach(new ForeachWriter[Row] {
          override def process(row: Row) {
            val timestamp = row.getAs[Long]("_timestamp")
            val point = Point.measurement(row.getAs[String]("channel"))
              .time(timestamp, TimeUnit.MILLISECONDS)
              .addField("value", row.getAs[Double]("value"))
              .tag("truck", row.getAs[String]("truck"))
              .build()
            influxDB.write(
              config.getConfigString(InfluxDBConnector.INFLUXDB_DATABASE),
              InfluxDBConnector.INFLUXDB_RETENTION_POLICY, point)
          }

          override def open(partitionId: Long, version: Long) = true

          override def close(errorOrNull: Throwable) {}
        })
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}
