package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}

import scala.collection.mutable.ListBuffer

object StatefulTruckAlerts {
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
        StructField("_timestamp", TimestampType, nullable = false),
        StructField("channel", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("unit", StringType, nullable = false)
      ))

      import sparkSession.implicits._

      // convert to json and select only channel 'T_motTemp_Lft'
      val channels = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema) as "channels").
        select($"channels.truck", $"channels.channel", $"channels._timestamp", $"channels.value").
        where("channel == 'T_motTemp_Lft'").
        as[Truck]

      // group by truck id and trigger an alert if condition is meet
      val alerts = channels.
        groupByKey(_.truck).
        mapGroupsWithState[TruckState, Alerts](GroupStateTimeout.NoTimeout) {

        case (truck: String, trucks: Iterator[Truck], state: GroupState[TruckState]) =>
          // If timed out, then remove session and send final update
          if (state.hasTimedOut) {
            val finalAlertUpdate = Alerts(truck, state.get.count, state.get.alerts, state.get.measurements,
              expired = true)
            state.remove()
            finalAlertUpdate
          } else {
            // Update state
            val truckUpdate = if (state.exists) {
              val oldSession = state.get
              TruckState(oldSession.trucks ++ trucks.toList, config.getDriverAlertThreshold)
            } else {
              TruckState(trucks.toList, config.getDriverAlertThreshold)
            }
            state.update(truckUpdate)
            //todo: figure out: Set timeout such that the session will be expired if no data received for 10 seconds
            //   state.setTimeoutDuration("5 seconds")
            Alerts(truck, state.get.count, state.get.alerts, state.get.measurements, expired = false)
          }
      }.withColumn("processing_time", current_timestamp()).where("count > 0")

      // Start running the query that prints the session updates to the console
      alerts
        .selectExpr("to_json(struct(processing_time, truck, alerts, measurements)) as json")
        .writeStream
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

// Defines the measurement, alert, and alerts, really just used to define the key in the json
case class Measurement(_timestamp: Timestamp, value: Double)

case class Alert(min: Measurement, max: Measurement)

//case class Alerts(alerts: List[Alert])

// User-defined data type representing the input events
case class Truck(truck: String, _timestamp: Timestamp, value: Double)

// User-defined data type for storing a truck information as state in mapGroupsWithState
case class TruckState(trucks: List[Truck], threshold: Int) {
  private val alertBuffer = new ListBuffer[Alert]()
  private val measurementBuffer = new ListBuffer[Measurement]()

  // set initial min value and time
  val initialTruck: Truck = trucks.head
  private var min = Measurement(initialTruck._timestamp, initialTruck.value)

  trucks.foreach(truck => {
    // check for alerts and collect alert points
    val currentTruck = Measurement(truck._timestamp, truck.value)
    if (currentTruck.value - min.value > threshold) {
      alertBuffer += Alert(min, currentTruck)
      // reset the min to the current truck that caused the alert
      min = currentTruck
    }
    // if current value is < existing min, reset the min
    if (currentTruck.value < min.value) min = currentTruck
    // collect all the values
    measurementBuffer += Measurement(truck._timestamp, truck.value)
  })

  def count: Long = alerts.size

  def alerts: List[Alert] = alertBuffer.toList

  def measurements: List[Measurement] = measurementBuffer.toList
}

// User-defined data type representing the update information returned by mapGroupsWithState
case class Alerts(truck: String,
                  count: Long,
                  alerts: List[Alert],
                  measurements: List[Measurement],
                  expired: Boolean)