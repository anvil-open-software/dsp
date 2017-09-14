package com.dematic.labs.dsp.drivers.kafka

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}


/**
  * Persist raw signals to Cassandra.
  */
object Persister {
  // should only be  used with testing
  private var injectedDriverConfiguration: DriverConfiguration = _

  private[drivers] def setDriverConfiguration(driverConfiguration: DriverConfiguration) {
    injectedDriverConfiguration = driverConfiguration
  }

  def main(args: Array[String]) {
    // driver configuration
    val config = if (injectedDriverConfiguration == null) {
      new DefaultDriverConfiguration.Builder().build
    } else {
      injectedDriverConfiguration
    }

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(config.getSparkMaster)) builder.master(config.getSparkMaster)
    builder.appName(config.getDriverAppName)
    builder.config("spark.cassandra.connection.host", config.getSparkCassandraConnectionHost)
    builder.config("spark.cassandra.connection.port", config.getSparkCassandraConnectionPort)
    builder.config("spark.cassandra.auth.username", config.getSparkCassandraAuthUsername)
    builder.config("spark.cassandra.auth.password", config.getSparkCassandraAuthPassword)
    val sparkSession: SparkSession = builder.getOrCreate

    // create the cassandra connector
    val connector: CassandraConnector = CassandraConnector.apply(sparkSession.sparkContext.getConf)

    // hook up Prometheus listener for monitoring
    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      sparkSession.streams.addListener(new PrometheusStreamingQueryListener(sparkSession.sparkContext.getConf,
        config.getDriverAppName))
    }

    // create the kafka input source and write to cassandra
    try {
      val kafka = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option(config.getKafkaSubscribe, config.getKafkaTopics)
        .option("startingOffsets", config.getKafkaStartingOffsets)
        .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger)
        .load

      // define the signal schema retrieve the signal values
      val schema: StructType = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("signalType", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false),
        StructField("producerId", StringType, nullable = true)
      ))

      import sparkSession.implicits._

      // convert to json and select all values
      val signals = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema).as("signals")).select("signals.*")

      // write to cassandra
      val persister = signals.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("persister")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            connector.withSessionDo { session =>
              session.execute(cql(row.getAs[Long]("id"), toTimeStamp(row.getAs[String]("timestamp")),
                row.getAs[String]("signalType"), row.getAs[Int]("value"), row.getAs[String]("producerId")))
            }
          }

          override def close(errorOrNull: Throwable) {}

          def cql(id: Long, timestamp: Timestamp, signalType: String, value: Int, producerId: String): String =
            s""" insert into ${config.getCassandraKeyspace}.signals (id, timestamp, signalType, value, producerId) values($id, '$timestamp', '$signalType', $value, '$producerId')"""
        }).start
      persister.awaitTermination()
    } finally
      sparkSession.close()
  }

  // todo: unify timestamps between cassandra and spark/sparks ql
  def toTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }
}
