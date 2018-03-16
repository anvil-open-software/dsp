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
  val INFLUXDB_DATABASE: String = "influxdb.database"
  val INFLUXDB_URL: String = "influxdb.url"
  val INFLUXDB_BATCH_COUNT: String = "influxdb.batch.count"
  val INFLUXDB_BATCH_FLUSH_SECONDS: String = "influxdb.batch.flush.duration.seconds"
  val INFLUXDB_RETENTION_POLICY: String = "autogen"

  val logger: Logger = LoggerFactory.getLogger("InfluxDBConnector")

  def initializeConnection(config: DriverConfiguration): InfluxDB = {
    val influx_database = config.getConfigString(InfluxDBConnector.INFLUXDB_DATABASE)

    // create the connection to influxDb with more generous timeout instead of default 10 seconds
    val httpClientBuilder = new OkHttpClient.Builder()
      .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

    val influxDB: InfluxDB = InfluxDBFactory.connect(
                               config.getConfigString(INFLUXDB_URL),
                               config.getConfigString("influxdb.username"),
                               config.getConfigString("influxdb.password"), httpClientBuilder)
       .setDatabase(influx_database);
    def batch_count = config.getConfigNumber(INFLUXDB_BATCH_COUNT);
    if (batch_count != null) {
      influxDB.enableBatch(batch_count.intValue(),
                             config.getConfigNumber(INFLUXDB_BATCH_FLUSH_SECONDS).intValue(),
                             TimeUnit.SECONDS)
    }


    if (influxDB.databaseExists(influx_database)) {
      logger.info("InfluxDB ready to use: " + influx_database);
    } else {
      throw new RuntimeException("Database does not exist " + influx_database)
    }
    return influxDB
  }
}
