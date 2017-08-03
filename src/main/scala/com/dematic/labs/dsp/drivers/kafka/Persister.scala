package com.dematic.labs.dsp.drivers.kafka

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.configuration.DriverConfiguration._
import com.google.common.base.Strings
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}


/**
  * Persist raw signals to Cassandra.
  */
object Persister {
  def main(args: Array[String]) {
    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(Spark.masterUrl)) builder.master(Spark.masterUrl)
    builder.appName(Driver.appName)
    builder.config(Spark.CassandraHostKey, Spark.cassandraHost)
    builder.config(Spark.CassandraUsernameKey, Spark.cassandraUsername)
    builder.config(Spark.CassandraPasswordKey, Spark.cassandraPassword)
    val sparkSession: SparkSession = builder.getOrCreate

    // create the cassandra connector
    val connector: CassandraConnector = CassandraConnector.apply(sparkSession.sparkContext.getConf)

    // hook up Prometheus listener for monitoring
    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      sparkSession.streams.addListener(new PrometheusStreamingQueryListener(sparkSession.sparkContext.getConf,Driver.appName))
    }

    // create the kafka input source and write to cassandra
    try {
      val kafka = sparkSession.readStream
        .format(Kafka.format())
        .option(Kafka.BootstrapServersKey, Kafka.bootstrapServers)
        .option(Kafka.subscribe, Kafka.topics)
        .option(removeQualifier(Kafka.StartingOffsetsKey), Kafka.startingOffsets)
        .option(removeQualifier(Kafka.MaxOffsetsPerTriggerKey), Kafka.maxOffsetsPerTrigger)
        .load

      /**
        * /* val schema = StructType(Seq(
        * StructField("aggregate_time", StructType(Seq(
        * StructField("start", TimestampType, false),
        * StructField("end", TimestampType, false)
        * ))),
        * StructField("opcTagId", LongType, false),
        * StructField("count", LongType, false),
        * StructField("avg", LongType, true),
        * StructField("min", LongType, true),
        * StructField("max", LongType, true),
        * StructField("sum", LongType, true)
        * ))*/
        */

      // define the signal schema retrieve the signal values
      val schema: StructType = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false),
        StructField("producerId", StringType, nullable = true)
      ))

      import sparkSession.implicits._

      // convert to json and select all values
      val signals = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema).as("signals")).select("signals.*")

      // write to cassandra
      val persister = signals.writeStream
        .option(Spark.CheckpointLocationKey, Spark.checkpointLocation)
        .queryName("persister")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            connector.withSessionDo { session =>
              session.execute(cql(row.getAs[Long]("id"), toTimeStamp(row.getAs[String]("timestamp")),
                row.getAs[Int]("value"), row.getAs[String]("producerId")))
            }
          }

          override def close(errorOrNull: Throwable) {}

          def cql(id: Long, timestamp: Timestamp, value: Int, producerId: String): String =
            s""" insert into ${Cassandra.keyspace}.signals (id, timestamp, value, producerId) values($id, '$timestamp', $value, '$producerId')"""

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
