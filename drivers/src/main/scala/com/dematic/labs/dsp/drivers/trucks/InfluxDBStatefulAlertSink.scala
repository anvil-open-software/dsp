package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.dematic.labs.analytics.monitor.spark.MonitorConsts
import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import com.dematic.labs.dsp.tsdb.influxdb.{InfluxDBConnector, InfluxDBConsts}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.dto.{BatchPoints, Point}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Converts truck alert json into influxdb request.
  * NOT a generic sink since the JSON format depends on truck message.
  */
class InfluxDBStatefulAlertSink(config: DriverConfiguration) extends ForeachWriter[Row] {

  override def process(row: Row) {
    // currently we take kafka until we get the child table ready
    val logger: Logger = LoggerFactory.getLogger("InfluxDBAlertSink")
    val points = BatchPoints.database(config.getConfigString(InfluxDBConsts.INFLUXDB_DATABASE))
      .tag("truck", row.getAs[String]("truck"))
      .tag("jenkins_job", System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID))
      .tag("cluster_id", System.getProperty(MonitorConsts.SPARK_CLUSTER_ID))
      .retentionPolicy(InfluxDBConsts.INFLUXDB_RETENTION_POLICY)
      .build()

    val actingTimestamp = row.getAs[Timestamp]("timestamp")
    val maxAlert = row.getAs[Row]("max")
    val minAlert = row.getAs[Row]("min")
    val maxTemp = maxAlert.getAs[Double]("value")
    val minTemp = minAlert.getAs[Double]("value")
    val minTime = minAlert.getAs[Timestamp]("timestamp")
    val maxTime = maxAlert.getAs[Timestamp]("timestamp")

    points.point(getPointBuilder("icd_stateful_alert", "alert_mode", "max_alert", maxTime)
      .addField("value", maxTemp).build())

    points.point(getPointBuilder("icd_stateful_alert", "alert_mode", "min_alert", minTime)
      .addField("value", minTemp).build())

    // difference (easier to do here for now instead of influx)
    points.point(getPointBuilder("icd_stateful_alert", "alert_mode", "diff_temp", maxTime)
      .addField("value", maxTemp - minTemp).build())

    // time diff in unix milliseconds
    points.point(getPointBuilder("icd_stateful_alert_time", "time_mode", "rise_time", maxTime)
      .addField("value", maxTime.getTime - minTime.getTime).build())
    InfluxDBConnector.getInfluxDbOrException.write(points)

    // array
    val measurements = row.getAs[mutable.WrappedArray[Row]]("measurements")
    measurements.foreach(row => {
      points.point(getPointBuilder("icd_recorded_temp", "temp", "T_motTemp_Lft",
        row.getAs[Timestamp]("timestamp"))
        .addField("value", row.getAs[Double]("value")).build())
    })
    try {
      InfluxDBConnector.getInfluxDbOrException.write(points)
    } catch {
      case t: Throwable => logger.error("Influx write failed for "+ points + " with " +t);
    }

  }

  def getPointBuilder(measurement: String, tag: String, metrictag: String, metricTime: Timestamp): Point.Builder = {
    Point.measurement(measurement)
      .tag(tag, metrictag).time(metricTime.getTime, TimeUnit.MILLISECONDS)

  }

  override def open(partitionId: Long, version: Long) = true

  override def close(errorOrNull: Throwable): Unit = {
  }
}
