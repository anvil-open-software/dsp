package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.time.Duration
import java.util.Locale

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}

import scala.collection.mutable.ListBuffer

object FlatMapTruckAlerts {
  // should only be  used with testing
  private var injectedDriverConfiguration: DriverConfiguration = _

  private[drivers] def setDriverConfiguration(driverConfiguration: DriverConfiguration) {
    injectedDriverConfiguration = driverConfiguration
  }

  // Defines the measurement, alert, and alerts, really just used to define the key in the json
  private case class Measurement(_timestamp: Timestamp, value: Double)

  private case class Alert(min: Measurement, max: Measurement)

  // User-defined data type representing the input events
  private case class Truck(truck: String, _timestamp: Timestamp, value: Double)

  // User-defined truck state
  private case class TruckState(trucks: List[Truck])

  // User-defined data type representing the update information returned by flatMapGroupsWithState
  private case class AlertRow(truck: String, alert: Alert, measurements: List[Measurement])

  // wrapper that contains trucks and alert rows
  private case class AlertWrapper(trucks: List[Truck], alertRows: Iterator[AlertRow])

  // Update function, takes a key, an iterator of trucks and a previous state, returns an iterator which represents the
  // rows of the output from flatMapGroupsWithState
  private def updateAlertsAcrossBatch(truck: String, newTrucks: Iterator[Truck], state: GroupState[TruckState]): Iterator[AlertRow] = {
    if (state.hasTimedOut) {
      // create final alerts
      val alerts = createAlerts(state.get.trucks).alertRows
      state.remove
      // return the iterator of final alert rows
      alerts
    } else {
      // Update state
      val updatedTrucks: List[Truck] = if (state.exists)
        state.get.trucks ++ newTrucks.toList
      else
        newTrucks.toList
      // create the updated alerts
      val alertWrapper = createAlerts(updatedTrucks)
      // update the state with the trucks
      state.update(TruckState(alertWrapper.trucks))
      // set the timeout to be last timestamp plus 60 min, a Timeout will eventually occur when there is a trigger
      // in the query, after X ms, basically, a Timeout occurs when the grouped key has not received any new data
      // and the time set of 60 min has elapsed
      if (alertWrapper.trucks.nonEmpty)
        state.setTimeoutTimestamp(alertWrapper.trucks.last._timestamp.getTime + 60 * 60 * 1000)
      // return the alerts
      alertWrapper.alertRows
    }
  }

  //noinspection ConvertExpressionToSAM
  // sort the trucks based on timestamp
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  private def createAlerts(trucks: List[Truck]): AlertWrapper = {
    val truckBuffer = new ListBuffer[Truck]()
    val alertBuffer = new ListBuffer[AlertRow]()

    // create the alerts and remove all values from ..
    val sortedTrucks = trucks.sortBy(truck => truck._timestamp)

    // set initial min value and time
    val initialTruck: Truck = sortedTrucks.head
    val truckId = initialTruck.truck
    // min measurement
    var min = Measurement(initialTruck._timestamp, initialTruck.value)

    sortedTrucks.foreach(truck => {
      val currentTruck = Measurement(truck._timestamp, truck.value)
      // first check if the min and current value time is less then an hour
      if (Duration.between(min._timestamp.toLocalDateTime, currentTruck._timestamp.toLocalDateTime).toHours < 1) {

        if (currentTruck.value - min.value > 10) {
          // create alert and reset min
          alertBuffer += AlertRow(truckId, Alert(min, currentTruck), List[Measurement]())
          min = currentTruck
        } else {
          // add to stateful truck list
          truckBuffer += truck
        }

        // if current value is < existing min, reset the min
        if (currentTruck.value < min.value) min = currentTruck
      } else {
        println(truck)
      }

    })
    AlertWrapper(truckBuffer.toList, alertBuffer.iterator)
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

      val alerts = channels.
        withWatermark("_timestamp", "60 minutes"). // how late the data can be before it is dropped
        groupByKey(_.truck).
        flatMapGroupsWithState[TruckState, AlertRow](outputMode(config.getSparkOutputMode),
        GroupStateTimeout.EventTimeTimeout)(updateAlertsAcrossBatch)

      // Start running the query that prints the session updates to the console
      alerts
        .selectExpr("to_json(struct(truck, alert, measurements)) AS value")
        .writeStream
        .format("console")
        .queryName("statefulTruckAlerts")
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option("topic", config.getKafkaOutputTopics)
        .option("checkpointLocation", config.getSparkCheckpointLocation)
        .option("truncate", "false")
        .outputMode(config.getSparkOutputMode)
        .start
      // keep alive
      sparkSession.streams.awaitAnyTermination
    } catch {
      case x: Throwable => {
        x.printStackTrace()
      }
    }
    finally
      sparkSession.close
  }

  private def outputMode(outputMode: String): OutputMode = {
    outputMode.toLowerCase(Locale.ROOT) match {
      case "append" =>
        OutputMode.Append
      case "complete" =>
        OutputMode.Complete
      case "update" =>
        OutputMode.Update
      case _ =>
        throw new IllegalArgumentException(s"Unknown output mode $outputMode. " +
          "Accepted output modes are 'append', 'complete', 'update'")
    }
  }
}
