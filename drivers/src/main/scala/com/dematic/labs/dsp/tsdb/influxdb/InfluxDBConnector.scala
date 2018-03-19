package com.dematic.labs.dsp.tsdb.influxdb

import java.util.concurrent.TimeUnit

import okhttp3.OkHttpClient
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Singleton for each jvm to batch influxdb requests
  *
  * We initialize the pool statically so it happens once per jvm.
  * If we use the lazy val pattern from the driver with initializeConnectionPool, we get called once per task.
  */
object InfluxDBConsts{
  val INFLUXDB_DATABASE: String = "influxdb.database"
  val INFLUXDB_URL: String = "influxdb.url"
  val INFLUXDB_BATCH_COUNT: String = "influxdb.batch.count"
  val INFLUXDB_USERNAME: String = "influxdb.username"
  val INFLUXDB_PASSWORD: String = "influxdb.password"
  val INFLUXDB_BATCH_FLUSH_SECONDS: String = "influxdb.batch.flush.duration.seconds"
  val INFLUXDB_RETENTION_POLICY: String = "autogen"
}

object InfluxDBConnector {

  val logger: Logger = LoggerFactory.getLogger("InfluxDBConnector")

  val influx_database = System.getProperty(InfluxDBConsts.INFLUXDB_DATABASE)

  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  val httpClientBuilder = new OkHttpClient.Builder()
    .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

  val influxDB: InfluxDB = InfluxDBFactory.connect(
    System.getProperty(InfluxDBConsts.INFLUXDB_URL),
    System.getProperty(InfluxDBConsts.INFLUXDB_USERNAME),
    System.getProperty(InfluxDBConsts.INFLUXDB_PASSWORD), httpClientBuilder)
    .setDatabase(influx_database)

  def batch_count = System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_COUNT)
  if (batch_count != null) {
    influxDB.enableBatch(Integer.valueOf(batch_count),
      Integer.valueOf(System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_FLUSH_SECONDS)),
      TimeUnit.SECONDS)
  }
  if (influxDB.databaseExists(influx_database)) {
    logger.info("InfluxDB ready to use: " + influx_database)
  } else {
    throw new RuntimeException("Database does not exist " + influx_database)
  }

  def getInfluxDB: InfluxDB = {
     influxDB
  }

  /**
    *
    * non-static configuration which does NOT work with "lazy val" pattern in driver
    *
  def initializeConnectionPool(config: DriverConfiguration): InfluxDB = {
    logger.info("default" + ConfigFactory.defaultApplication().hasPath(InfluxDBConnector.INFLUXDB_DATABASE));
    val influx_database = config.getConfigString(InfluxDBConnector.INFLUXDB_DATABASE)

    // create the connection to influxDb with more generous timeout instead of default 10 seconds
    val httpClientBuilder = new OkHttpClient.Builder()
      .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

    val influxDB: InfluxDB = InfluxDBFactory.connect(
                               config.getConfigString(INFLUXDB_URL),
                               config.getConfigString(INFLUXDB_USERNAME),
                               config.getConfigString(INFLUXDB_PASSWORD), httpClientBuilder)
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
    **/
}
