package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}

import scala.collection.mutable.ListBuffer

object StatefulTruckAlert {
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
            val finalAlertUpdate = Alerts(truck, state.get.alertCount, state.get.alertValues, expired = true)
            state.remove()
            finalAlertUpdate
          } else {
            // Update state
            val truckUpdate = if (state.exists) {

              val oldSession = state.get
              TruckState(oldSession.trucks ++ trucks.toList)

            } else {
              TruckState(trucks.toList)
            }

            state.update(truckUpdate)

            //todo: figure out: Set timeout such that the session will be expired if no data received for 10 seconds
            //   state.setTimeoutDuration("5 seconds")
            Alerts(truck, state.get.alertCount, state.get.alertValues, expired = false)

          }
      }.where("alerts > 0")

      // Start running the query that prints the session updates to the console
      alerts
        .writeStream
        .outputMode("update")
        .format("console")
        .start()


      // write results to kafka


      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }

}

/** User-defined data type representing the input events */
case class Truck(truck: String, _timestamp: Timestamp, value: Double)

/**
  * User-defined data type for storing a truck information as state in mapGroupsWithState.
  *
  */
case class TruckState(trucks: List[Truck]) {
  private val points = new ListBuffer[((Timestamp, Double), (Timestamp, Double))]()
  private val values = new ListBuffer[(Timestamp, Double)]()

  // set initial min value and time
  val initialTruck: Truck = trucks.head
  private var min = (initialTruck._timestamp, initialTruck.value)

  trucks.foreach(truck => {
    // check for alerts and collect alert points
    val currentTruck = (truck._timestamp, truck.value)
    if (currentTruck._2 - min._2 > 10) {
      points += Tuple2(min, currentTruck)
      // reset the min to the current truck that caused the alert
      min = currentTruck
    }
    // if current value is < existing min, reset the min
    if (currentTruck._2 < min._2) min = currentTruck
    // collect all the values
    values += Tuple2(truck._timestamp, truck.value)
  })


  def alertCount: Long = points.size / 2

  def alertPoints: ListBuffer[((Timestamp, Double), (Timestamp, Double))] = points

  def alertValues: List[(Timestamp, Double)] = values.toList
}


/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  */
case class Alerts(truck: String,
                  alerts: Long,
                  values: List[(Timestamp, Double)],
                  expired: Boolean)