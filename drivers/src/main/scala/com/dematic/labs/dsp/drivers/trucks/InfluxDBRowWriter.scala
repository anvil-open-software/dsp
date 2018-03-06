package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import okhttp3.OkHttpClient
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.Point

/**
  *  write batched points
  */
class InfluxDBRowWriter extends ForeachWriter[Row] {

  override def open(partitionId: Long, version: Long) = true

  override def process(row: Row) {
    val timestamp: Timestamp= row.getAs[Timestamp]("_timestamp")
    val point = Point.measurement(row.getAs[String]("T_motTemp_Lft"))
      .time(timestamp.getTime, TimeUnit.MILLISECONDS)
      .addField("T_motTemp_Lft", row.getAs[Double]("value"))
      .tag("truck",  row.getAs[String]("truck") )
      .build();
    InfluxDB.influxDB.write(point)

  }
  override def close(errorOrNull: Throwable) {}
}

private object InfluxDB {
  // todo hook in parms if it works
  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  val httpClientBuilder = new OkHttpClient.Builder()
    .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

  val influxDB: InfluxDB = InfluxDBFactory.connect("http://10.207.208.10:8086", "kafka", "kafka1234", httpClientBuilder)
    .enableBatch(5000, 3, TimeUnit.SECONDS);
  influxDB.setDatabase("ccd_output")
}