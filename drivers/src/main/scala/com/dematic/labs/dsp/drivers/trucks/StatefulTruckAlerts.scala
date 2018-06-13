package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.time.Duration
import java.util.Locale

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.dematic.labs.dsp.tsdb.influxdb.InfluxDBConnector
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}
import org.influxdb.dto.Query
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object StatefulTruckAlerts {
  private val logger: Logger = LoggerFactory.getLogger("StatefulTruckAlerts")
  // alert threshold, default to 10
  private val THRESHOLD: Int = System.getProperty("driver.alert.threshold", "10").toInt
  // should only be  used with testing
  private var injectedDriverConfiguration: DriverConfiguration = _

  private[drivers] def setDriverConfiguration(driverConfiguration: DriverConfiguration) {
    injectedDriverConfiguration = driverConfiguration
  }

  // Defines the measurement, alert, and alerts, really just used to define the key in the json
  private case class Measurement(timestamp: String, value: Double)

  // User-defined data type representing the input events
  private case class Truck(truck: String, _timestamp: Timestamp, value: Double)

  // User-defined truck state
  private case class TruckState(min: Truck)

  // User-defined data type representing the update information returned by flatMapGroupsWithState
  private case class AlertRow(truck: String, min: Measurement, max: Measurement, measurements: List[Measurement])

  // wrapper that contains trucks and alert rows
  private case class AlertWrapper(min: Truck, alertRows: Iterator[AlertRow])

  //noinspection ConvertExpressionToSAM
  // sort the trucks based on timestamp
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  // Update function, takes a key, an iterator of trucks and a previous state, returns an iterator which represents the
  // rows of the output from flatMapGroupsWithState
  private def updateAlertsAcrossBatch(truck: String, newTrucks: Iterator[Truck], state: GroupState[TruckState]): Iterator[AlertRow] = {
    if (state.hasTimedOut) {
      // create final alerts
      val alerts = createAlerts(state.get.min, List.empty[Truck]).alertRows
      state.remove
      // return the iterator of final alert rows
      alerts
    } else {
      // sort the trucks and set the timeout
      val sorted: List[Truck] = newTrucks.toList.sortBy(truck => truck._timestamp)
      // set the timeout to be last timestamp plus 60 min, a Timeout will eventually occur when there is a trigger
      // in the query, after X ms, basically, a Timeout occurs when the grouped key has not received any new data
      // and the time set of 60 min has elapsed
      if (sorted.nonEmpty) state.setTimeoutTimestamp(sorted.last._timestamp.getTime + 60 * 60 * 1000)
      // get min
      val min: Truck = if (state.exists) state.get.min else sorted.head
      // create the updated alerts
      val alertWrapper = createAlerts(min, sorted)
      // update the state with the trucks
      state.update(TruckState(alertWrapper.min))
      // return the alerts
      alertWrapper.alertRows
    }
  }

  private def createAlerts(min: Truck, trucks: List[Truck]): AlertWrapper = {
    val alertBuffer = new ListBuffer[AlertRow]()
    // min changes over time as we go through the list of truck values
    val newMin: Truck = calculateAlerts(min, trucks, alertBuffer)
    AlertWrapper(newMin, alertBuffer.iterator)
  }

  private def calculateAlerts(min: Truck, trucks: List[Truck], alertBuffer: ListBuffer[AlertRow]): Truck = {
    var newMin: Truck = min
    val influxDBConnector = InfluxDBConnector.getInfluxDB

    trucks.foreach(t => {
      val currentTime = t._timestamp
      val currentValue = t.value
      if (isTimeWithInHour(newMin._timestamp, currentTime)) {
        // calculate alerts if they exist
        if (currentValue - newMin.value > THRESHOLD && currentTime.after(newMin._timestamp)) {
          // create the lower time boundary
          val results =
            influxDBConnector.query(
              new Query(
                s"""select time, value from T_motTemp_Lft where truck='${t.truck}' and time >= '$currentTime' - 60m and time <= '$currentTime'""",
                InfluxDBConnector.getDatabase))
          //todo: deal with errors and no results
          // log any errors
          if (results.hasError) logger.error(results.getError)
          // create alerts
          if (!results.getResults.isEmpty) {
            val nestedResults = results.getResults.get(0)
            val series = nestedResults.getSeries
            if (!series.isEmpty) {
              val singleSeries = series.get(0)
              val values = singleSeries.getValues
              if (!values.isEmpty) {
                // populate the measurements
                val measurements =
                  values.toList.map((f: java.util.List[AnyRef]) =>
                    Measurement(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[Double])): List[Measurement]
                // create the alert buffer
                alertBuffer += AlertRow(t.truck, Measurement(newMin._timestamp.toString, newMin.value),
                  Measurement(currentTime.toString, currentValue), measurements)
              }
            }
          }
          // reset the min
          newMin = t
        }
        // current value is < existing min, reset the min
        if (t._timestamp.after(newMin._timestamp) && t.value < newMin.value) newMin = t
      } else {
        newMin = t
      }
    })
    newMin
  }

  private def isTimeWithInHour(time1: Timestamp, time2: Timestamp): Boolean = Duration.between(time1.toLocalDateTime,
    time2.toLocalDateTime).toHours < 1

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
        .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger)
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
        withWatermark("_timestamp", "1 minutes"). // how late the data can be before it is dropped
        groupByKey(_.truck).
        flatMapGroupsWithState[TruckState, AlertRow](outputMode(config.getSparkOutputMode),
        GroupStateTimeout.EventTimeTimeout)(updateAlertsAcrossBatch)

      // Start running the query that prints the session updates to the console
      alerts
        .selectExpr("truck as key", "to_json(struct(truck, min, max, measurements)) AS value")
        .writeStream
        .format("kafka")
        .queryName("statefulTruckAlerts")
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