package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import com.dematic.labs.dsp.tsdb.influxdb.InfluxDBConnector
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

/**
  * This is not a generic sink since the write format depends on truck message
  */
class InfluxDBSink(config:DriverConfiguration, influxDB:InfluxDB) extends ForeachWriter[Row] {

    override def process(row: Row) {
      val timestamp = row.getAs[Timestamp]("_timestamp")
      val point = Point.measurement(row.getAs[String]("channel"))
        .time(timestamp.getTime, TimeUnit.MILLISECONDS)
        .addField("value", row.getAs[Double]("value"))
        .tag("truck", row.getAs[String]("truck"))
        .build()
      influxDB.write( config.getConfigString(InfluxDBConnector.INFLUXDB_DATABASE),
        InfluxDBConnector.INFLUXDB_RETENTION_POLICY, point)
    }

    override def open(partitionId: Long, version: Long) = true

    override def close(errorOrNull: Throwable): Unit = {
      //  influxDB.close()
    }
  }
