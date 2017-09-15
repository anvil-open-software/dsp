package com.dematic.labs.dsp.drivers.kafka

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

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

      // write sorter signals to kafka topic :todo
      sorters.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("sorters")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            println(row)
          }

          override def close(errorOrNull: Throwable) {}

        }).start

      val pickers = signals.select("*").where("signalType == 'Picker'")
      // write picker signals to kafka topic :todo
      pickers.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("pickers")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            println(row)
          }

          override def close(errorOrNull: Throwable) {}

        }).start
      // keep alive
      sparkSession.streams.awaitAnyTermination()
    } catch {
      // todo: remove
      case e: Exception => e.printStackTrace()
    } finally
      sparkSession.close()
  }
}
