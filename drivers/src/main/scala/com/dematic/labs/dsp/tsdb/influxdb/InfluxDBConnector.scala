package com.dematic.labs.dsp.tsdb.influxdb

import java.util.concurrent.TimeUnit

import okhttp3.OkHttpClient
import org.influxdb.dto.{Query, QueryResult}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Singleton for each jvm to batch influxdb requests
  *
  * We initialize the pool statically so it happens once per jvm.
  * If we use the lazy val pattern from the driver with initializeConnectionPool, we get called once per task.
  */
object InfluxDBConsts {
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

  private val influx_database: String = System.getProperty(InfluxDBConsts.INFLUXDB_DATABASE)
  private val batch_count: String = System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_COUNT)
  private val batch_flush_seconds: String = System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_FLUSH_SECONDS)

  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  private val httpClientBuilder: OkHttpClient.Builder = new OkHttpClient.Builder()
    .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

  private val influxDB: Either[Throwable, InfluxDB] = {
    try {
      val connection = InfluxDBFactory.connect(
        System.getProperty(InfluxDBConsts.INFLUXDB_URL),
        System.getProperty(InfluxDBConsts.INFLUXDB_USERNAME),
        System.getProperty(InfluxDBConsts.INFLUXDB_PASSWORD), httpClientBuilder)
        .setDatabase(influx_database)
      // validate the connection
      connection.ping
      // validate the database
      connection.databaseExists(influx_database)
      // return and set connection
      Right(connection)
    } catch {
      case t: Throwable => Left(t);
    }
  }

  // set the batch count if exist
  if (batch_count != null && batch_flush_seconds != null) {
    influxDB.right.get.enableBatch(Integer.valueOf(batch_count),
      Integer.valueOf(System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_FLUSH_SECONDS)), TimeUnit.SECONDS)
  } else {
    logger.info(s"""batch count='$batch_count' and batch flush seconds='$batch_flush_seconds'""")
  }

  /**
    * Get the InfluxDB.
    *
    * @return InfluxDB or exception if can't make connection
    */
  def getInfluxDbOrException: InfluxDB = {
    if (influxDB.isRight) influxDB.right.get else throw influxDB.left.get
  }

  def getDatabase: String = influx_database

  def query(query: String): Either[Throwable, QueryResult] = {
    try {
      Right(getInfluxDbOrException.query(new Query(query, getDatabase)))
    } catch {
      case t: Throwable => Left(t)
    }
  }
}
