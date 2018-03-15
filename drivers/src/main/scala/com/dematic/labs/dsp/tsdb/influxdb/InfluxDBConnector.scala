package com.dematic.labs.dsp.tsdb.influxdb

import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.drivers.configuration.DriverConfiguration
import okhttp3.OkHttpClient
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  */
object InfluxDBConnector {
  val INFLUX_DATABASE: String = "influx.database"

  val logger: Logger = LoggerFactory.getLogger("InfluxDBConnector")

  def initializeConnection( config: DriverConfiguration): InfluxDB = {
    val influx_database = config.getConfigString(InfluxDBConnector.INFLUX_DATABASE)

    // create the connection to influxDb with more generous timeout instead of default 10 seconds
    val httpClientBuilder = new OkHttpClient.Builder()
      .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

    val influxDB: InfluxDB = InfluxDBFactory.connect("influxdb.url", config.getConfigString("influxdb.database"),
                                 config.getConfigString("influxdb.password"), httpClientBuilder)
      .setDatabase(influx_database)
      .enableBatch(5000, 3, TimeUnit.SECONDS)

    if (influxDB.databaseExists(influx_database)) {
      logger.info("InfluxDB ready to use: " + influx_database);
    } else {
      throw new RuntimeException("Database does not exist " + influx_database)
    }
    return influxDB
  }
}
