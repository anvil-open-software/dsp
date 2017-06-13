package com.dematic.labs.dsp.drivers.kafka

import java.sql.Timestamp

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.dsp.configuration.DriverConfiguration.{Cassandra, Driver, Kafka, Spark}
import com.dematic.labs.dsp.data.Utils
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

    // create the kafka input source and write to cassandra
    try {
      val kafka = sparkSession.readStream
        .format(Kafka.format())
        .option(Kafka.BootstrapServersKey, Kafka.bootstrapServers)
        .option(Kafka.subscribe, Kafka.topics)
        .option(Kafka.TopicSubscriptionKey, Kafka.startingOffsets)
        .load

      // define the signal schema retrieve the signal values
      val schema: StructType = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false),
        StructField("generatorId", StringType, nullable = true)
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
              session.execute(cql(row.getAs[Long]("id"), Utils.toTimeStamp(row.getAs[String]("timestamp")),
                row.getAs[Int]("value"), row.getAs[String]("generatorId")))
            }
          }

          override def close(errorOrNull: Throwable) {}

          def cql(id: Long, timestamp: Timestamp, value: Int, generatorId: String): String =
            s""" insert into ${Cassandra.keyspace}.signals (id, timestamp, value, generatedId) values($id, $timestamp, $value, '$generatorId')"""

        }).start
      persister.awaitTermination()
    } finally
      sparkSession.close()
  }
}
