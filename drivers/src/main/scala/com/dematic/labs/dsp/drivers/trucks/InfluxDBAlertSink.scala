/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.dematic.labs.analytics.monitor.spark.MonitorConsts
import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import com.dematic.labs.dsp.tsdb.influxdb.{InfluxDBConnector, InfluxDBConsts}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.dto.Point

/**
  * Converts truck alert json into influxdb request.
  * NOT a generic sink since the JSON format depends on truck message.
  */
class InfluxDBAlertSink(config: DriverConfiguration) extends ForeachWriter[Row] {

  override def process(row: Row) {
    // currently we take kafka until we get the child table ready
    val actingTimestamp = row.getAs[Timestamp]("timestamp")
    val point = Point.measurement("icd_alert_lift_motor_temp")
      .time(actingTimestamp.getTime, TimeUnit.MILLISECONDS)
      .addField("value", row.getAs[Long]("alerts"))
      .tag("truck", row.getAs[String]("truck"))
      .tag("jenkins_job",  System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID))
      .tag("cluster_id",System.getProperty(MonitorConsts.SPARK_CLUSTER_ID))
      .build()
    InfluxDBConnector.getInfluxDbOrException.write(config.getConfigString(InfluxDBConsts.INFLUXDB_DATABASE),
      InfluxDBConsts.INFLUXDB_RETENTION_POLICY, point)

  }

  override def open(partitionId: Long, version: Long) = true

  override def close(errorOrNull: Throwable): Unit = {
  }
}
