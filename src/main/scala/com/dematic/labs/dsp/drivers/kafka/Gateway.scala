package com.dematic.labs.dsp.drivers.kafka

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object Gateway {
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

    // create the kafka input source and write to cassandra
    try {
      val kafka = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option(config.getKafkaSubscribe, config.getKafkaTopics)
        .option("startingOffsets", config.getKafkaStartingOffsets)
        .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger)
        .load

      // define the signal schema retrieve the signal values
      val schema: StructType = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("signalType", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false),
        StructField("producerId", StringType, nullable = true)
      ))

      import sparkSession.implicits._

      // convert to json and select all values
      val signals = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema).as("signals")).select("signals.*")

      val sorters = signals.select("*").where("signalType == 'Sorter'")

      sorters.selectExpr("CAST(value AS STRING)").writeStream
        .format("kafka")
        .queryName("sorter")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option("topic", "sorter")
        .option("checkpointLocation", config.getSparkCheckpointLocation + "/sorter")
        .start

      val pickers = signals.select("*").where("signalType == 'Picker'")

      pickers.selectExpr("CAST(value AS STRING)").writeStream
        .format("kafka")
        .queryName("picker")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option("topic", "pickers")
        .option("checkpointLocation", config.getSparkCheckpointLocation + "/picker")
        .start

      pickers.selectExpr("CAST(value AS STRING)").writeStream
        .format("kafka")
        .queryName("dms")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option("topic", "dms")
        .option("checkpointLocation", config.getSparkCheckpointLocation + "/dms")
        .start
      // keep alive
      sparkSession.streams.awaitAnyTermination()
    } finally
      sparkSession.close()
  }
}
