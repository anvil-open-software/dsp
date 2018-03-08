package com.dematic.labs.dsp.drivers.trucks

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import okhttp3.OkHttpClient
import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.Point
import org.slf4j.{Logger, LoggerFactory}

/**
  *  write batched points
  *
  *  might want to put in error handler- errors now just go to log
  */
class InfluxDBRowWriter extends ForeachWriter[Row] {
  val logger: Logger = LoggerFactory.getLogger("InfluxDBRowWriter")
  override def open(partitionId: Long, version: Long) = true

  override def process(row: Row) {
    val timestamp = row.getAs[Long]("_timestamp")
    val point = Point.measurement(row.getAs[String]("channel"))
      .time(timestamp, TimeUnit.MILLISECONDS)
      .addField("value", row.getAs[Double]("value"))
      .tag("truck",  row.getAs[String]("truck") )
      .build();
    logger.info(point.toString);
    InfluxDB.influxDB.write(InfluxDB.influx_database, "autogen",point)

  }
  override def close(errorOrNull: Throwable) {}
}

protected object InfluxDB {
  val logger: Logger = LoggerFactory.getLogger("InfluxDB")
  val influx_database = "ccd_output";

  // todo hook in parms if it works
  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  val httpClientBuilder = new OkHttpClient.Builder()
    .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

  val influxDB: InfluxDB = InfluxDBFactory.connect("http://10.207.208.10:8086", "kafka", "kafka1234", httpClientBuilder)
    .setDatabase(influx_database)
    .enableBatch(5000, 3, TimeUnit.SECONDS)

  def validateConnection(): Unit = {
    if (influxDB.databaseExists(influx_database)) {
      logger.info("InfluxDB ready to use: " + influx_database);
    } else {
      throw new RuntimeException("Database does not exist " + influx_database)
    }

  }

}