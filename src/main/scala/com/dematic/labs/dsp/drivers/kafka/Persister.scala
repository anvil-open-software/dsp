package com.dematic.labs.dsp.drivers.kafka

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.dsp.configuration.DriverConfiguration.{Driver, Kafka, Spark}
import com.google.common.base.Strings
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

/**
  * Persist raw signals to Cassandra.
  */
object Persister {
  def main(args: Array[String]) {
    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(Spark.masterUrl)) builder.master(Spark.masterUrl)
    builder.appName(Driver.appName)
    val sparkSession: SparkSession = builder.getOrCreate

    // create the cassandra connector
    val connector: CassandraConnector = CassandraConnector.apply(sparkSession.sparkContext.getConf)

    // create the kafka input source and write to cassandra
    try {
      val persister = sparkSession.readStream
        .format(Kafka.format())
        .option(Kafka.BootstrapServersKey, Kafka.bootstrapServers)
        .option(Kafka.subscribe, Kafka.topics)
        .option(Kafka.TopicSubscriptionKey, Kafka.startingOffsets)
        .load
        // write to cassandra
        .writeStream
        .option(Spark.CheckpointLocationKey, Spark.checkpointLocation)
        .queryName("persister")
        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(value: Row) {
            connector.withSessionDo { session => session.execute("") }
          }

          override def close(errorOrNull: Throwable) {}
        }).start
      persister.awaitTermination()
    } finally
      sparkSession.stop()
  }
}
