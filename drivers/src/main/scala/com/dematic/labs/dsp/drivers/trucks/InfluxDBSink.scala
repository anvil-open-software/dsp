package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.dematic.labs.analytics.monitor.spark.MonitorConsts
import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import com.dematic.labs.dsp.tsdb.influxdb.{InfluxDBConnector, InfluxDBConsts}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.dto.Point

/**
  * Converts truck json into influx request. This is not a generic sink since the write format depends on truck message.
  */
class InfluxDBSink(config:DriverConfiguration) extends ForeachWriter[Row] {

    override def process(row: Row) {
      val timestamp = row.getAs[Timestamp]("_timestamp")
      val point = Point.measurement(row.getAs[String]("channel"))
        .time(timestamp.getTime, TimeUnit.MILLISECONDS)
        .addField("value", row.getAs[Double]("value"))
        .tag("truck", row.getAs[String]("truck"))
        .tag("jenkins_job",  System.getProperty(MonitorConsts.SPARK_DRIVER_UNIQUE_RUN_ID))
        .tag("cluster_id",System.getProperty(MonitorConsts.SPARK_CLUSTER_ID))
        .build()
      InfluxDBConnector.getInfluxDB.write( config.getConfigString(InfluxDBConsts.INFLUXDB_DATABASE),
        InfluxDBConsts.INFLUXDB_RETENTION_POLICY, point)
    }

    override def open(partitionId: Long, version: Long) = true

    override def close(errorOrNull: Throwable): Unit = {
    }
  }
