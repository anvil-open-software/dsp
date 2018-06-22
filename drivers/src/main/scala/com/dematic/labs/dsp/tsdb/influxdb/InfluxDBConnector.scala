package com.dematic.labs.dsp.tsdb.influxdb

import java.util.concurrent.TimeUnit

import okhttp3.OkHttpClient
import org.influxdb.dto.QueryResult
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

  private val logger: Logger = LoggerFactory.getLogger("InfluxDBConnector")

  private val influx_database: String = System.getProperty(InfluxDBConsts.INFLUXDB_DATABASE)
  private val batch_count: String = System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_COUNT)

  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  private val httpClientBuilder: OkHttpClient.Builder = new OkHttpClient.Builder()
    .writeTimeout(120, TimeUnit.SECONDS).connectTimeout(120, TimeUnit.SECONDS)

  private val influxDB: Either[Exception, InfluxDB] = {
    try {
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
        // set the batch count if exist
        if (batch_count != null)
          influxDB.right.get.enableBatch(Integer.valueOf(batch_count),
            Integer.valueOf(System.getProperty(InfluxDBConsts.INFLUXDB_BATCH_FLUSH_SECONDS)), TimeUnit.SECONDS)
        // return and set connection
        Right(connection)
      }
    } catch {
      case e: Exception => Left(e);
    }
  }

  /**
    * Get the InfluxDB.
    *
    * @return InfluxDB or exception if can't make connection
    */
  def getInfluxDbOrException: InfluxDB = {
    if (influxDB.isRight) influxDB.right.get else throw influxDB.left.get
  }

  /**
    * Get the InfluxDB.
    *
    * @return InfluxDB or 'null' if can't make connection
    */
  def getInfluxDbOrNull: InfluxDB = {
    if (influxDB.isRight) influxDB.right.get else {
      logger.error("Can't connect to InfluxDB", influxDB.left.get)
      null
    }
  }

  def getDatabase: String = influx_database

  def query(query:String, throwException: Boolean): QueryResult = {
    if(throwException) {
      influxDB.isRight
    }
    null
  }

}
