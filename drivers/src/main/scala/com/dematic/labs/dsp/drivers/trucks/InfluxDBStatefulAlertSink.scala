package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.dematic.labs.analytics.monitor.spark.MonitorConsts
import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import com.dematic.labs.dsp.tsdb.influxdb.{InfluxDBConnector, InfluxDBConsts}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.InfluxDB
import org.influxdb.dto.{BatchPoints, Point}

/**
  * Converts truck alert json into influxdb request.
  * NOT a generic sink since the JSON format depends on truck message.
  */
class InfluxDBStatefulAlertSink(config: DriverConfiguration) extends ForeachWriter[Row] {

  override def process(row: Row) {
    // currently we take kafka until we get the child table ready

    val points= BatchPoints.database(config.getConfigString(InfluxDBConsts.INFLUXDB_DATABASE))
      .tag("truck", row.getAs[String]("truck"))
      .tag("jenkins_job",System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID))
      .tag("cluster_id",System.getProperty(MonitorConsts.SPARK_CLUSTER_ID))
      .retentionPolicy(InfluxDBConsts.INFLUXDB_RETENTION_POLICY)
      .build()

    val actingTimestamp = row.getAs[Timestamp]("timestamp")
    val maxAlert = row.getAs[Row]("max")
    val minAlert = row.getAs[Row]("min")
    val maxTemp = maxAlert.getAs[Double]("value")
    val minTemp =  minAlert.getAs[Double]("value")

    points.point(getPointBuilder("max_alert",maxAlert.getAs[Timestamp]("_timestamp"))
                  .addField("value", maxTemp).build())

    points.point(getPointBuilder("min_alert",minAlert.getAs[Timestamp]("_timestamp"))
      .addField("value", minTemp).build())

    // difference (easier to do here for now instead of influx)
    points.point(getPointBuilder("diff",maxAlert.getAs[Timestamp]("_timestamp"))
      .addField("value", maxTemp-minTemp).build())

    InfluxDBConnector.getInfluxDB.write(points)

  }
  def getPointBuilder( metrictag:String, metricTime:Timestamp): Point.Builder = {
    Point.measurement("icd_stateful_alert")
        .tag("alert_mode", metrictag)
      .time(metricTime.getTime, TimeUnit.MILLISECONDS)

  }
  override def open(partitionId: Long, version: Long) = true

  override def close(errorOrNull: Throwable): Unit = {
  }
}
