package com.dematic.labs.dsp.drivers.kafka

import java.sql.Timestamp

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.dsp.configuration.DriverConfiguration.{Driver, Kafka, Spark}
import com.dematic.labs.dsp.data.{Signal, Utils}
import com.google.common.base.Strings
import org.apache.spark.sql.{Encoders, ForeachWriter, SparkSession}

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

      // retrieve the signal values
      val json = kafka.selectExpr("CAST(value AS STRING)").as(Encoders.STRING)

      // explicitly define signal encoders
      implicit val encoder = Encoders.bean[Signal](classOf[Signal])

      // convert to signals
      val signals = json.as[Signal]

      // write to cassandra
      val persister = signals.writeStream
        .option(Spark.CheckpointLocationKey, Spark.checkpointLocation)
        .queryName("persister")
        .foreach(new ForeachWriter[Signal]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(signal: Signal) {
            // spark.cassandra.connection.host"
            connector.withSessionDo { session =>
              session.execute(cql(signal.id, Utils.toTimeStamp(signal.timestamp), signal.value, signal.generatorId))
            }
          }

          override def close(errorOrNull: Throwable) {}

          def cql(id: Long, time: Timestamp, value: Int, generatorId: String): String =
            s""" insert into my_keyspace.test_table (id, time, value, generatedId) values('$id', '$time', '$value', '$generatorId')"""

        }).start
      persister.awaitTermination()
    } finally
      sparkSession.close()
  }
}
