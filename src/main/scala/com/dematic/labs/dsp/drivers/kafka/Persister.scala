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
    val spark: SparkSession = builder.getOrCreate

    // create the input source, kafka
    val kafka = spark.readStream
      .format(Kafka.format())
      .option(Kafka.BootstrapServersKey, Kafka.bootstrapServers)
      .option(Kafka.subscribe, Kafka.topics)
      .option(Kafka.TopicSubscriptionKey, Kafka.startingOffsets)
      .load

    // save the raw signals in Cassandra
    val query = kafka.writeStream
      .option(Spark.CheckpointLocationKey, Spark.checkpointLocation)
      .queryName("persister")
      .foreach(new ForeachWriter[Row]() {
        private val connector: CassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)

        override def open(partitionId: Long, version: Long) {}

        override def process(value: Row) {}

        override def close(errorOrNull: Throwable) {}
      }).start
    query.awaitTermination
  }
}
