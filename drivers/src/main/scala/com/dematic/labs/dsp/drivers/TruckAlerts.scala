package com.dematic.labs.dsp.drivers

import java.sql.Timestamp

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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

    val increasingTemperatureAlert = new IncreasingTemperatureAlert(10) // todo: make configurable
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
        withWatermark("_timestamp", "1 minutes"). // only keep old data for 1 minutes for late updates
        groupBy(window('_timestamp, "60 minutes") as "alert_time", 'truck).
        //   agg(collect_list(struct('_timestamp, 'value)))
        agg(increasingTemperatureAlert('_timestamp, 'value))

      // just write to the console
      alerts.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("truckAlerts")
        .outputMode("update")

        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            println(row.toString())
          }

          override def close(errorOrNull: Throwable) {}

        })
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}

private class IncreasingTemperatureAlert(threshold: Int) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      Array(
        StructField("_timestamp", TimestampType),
        StructField("value", DoubleType)
      )
    )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    Array(
      StructField("min", DoubleType),
      StructField("alerts", ArrayType(
        StructType(
          Array(
            StructField("timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    Array(
      StructField("alerts", ArrayType(
        StructType(
          Array(
            StructField("timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null // min
    buffer.update(1, Array.empty[(Timestamp, Double)]) // empty list of t/v
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // update the min
    updateMin(buffer, input)
    // update the buffer
    updateBuffer(buffer, input)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    mergeMin(buffer1, buffer2)
    mergeBuffer(buffer1, buffer2)
  }

  //noinspection ConvertExpressionToSAM
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val min = buffer.getDouble(0)
    //todo: sort by timestamp, ensure it is efficient
    val values = buffer.getList[GenericRowWithSchema](1) sortBy (time => time.getTimestamp(0))
  }

  private def updateMin(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // keep the minimum
    val inputMin = input.getDouble(1) // input row is t/v
    // check to see if min has been set, internal structure is min, t/v
    buffer(0) =
      if (buffer.isNullAt(0)) inputMin
      else if (inputMin > buffer.getDouble(0)) buffer.getDouble(0)
      else inputMin
  }

  private def mergeMin(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // internal structure is min, t/v
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }
    if (buffer1(0) != null) {
      buffer1(0) = if (buffer1.getDouble(0) > buffer2.getDouble(0)) buffer2.getDouble(0) else buffer1.getDouble(0)
    }
  }

  private def updateBuffer(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // internal structure is min, t/v
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer.getAs[List[(Timestamp, Double)]](1)
    val inputValues: (Timestamp, Double) = (input.getTimestamp(0), input.getDouble(1))
    tempArray += inputValues
    buffer.update(1, tempArray)
  }

  private def mergeBuffer(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer1.getAs[List[(Timestamp, Double)]](1)
    tempArray ++= buffer2.getAs[List[(Timestamp, Double)]](1)
    buffer1.update(1, tempArray)
  }
}
