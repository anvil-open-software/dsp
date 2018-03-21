package com.dematic.labs.dsp.drivers

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object TruckAlerts {
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
    val sparkSession: SparkSession = builder.getOrCreate

    // hook up Prometheus listener for monitoring
    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      sparkSession.streams.addListener(new PrometheusStreamingQueryListener(sparkSession.sparkContext.getConf,
        config.getDriverAppName))
    }

    val increasingTemperatureAlert = new IncreasingTemperatureAlert(10) // todo: make configurable
    // create the kafka input source
    try {
      val kafka = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers)
        .option(config.getKafkaSubscribe, config.getKafkaTopics)
        .option("startingOffsets", config.getKafkaStartingOffsets)
        .load

      // define the truck json schema
      val schema: StructType = StructType(Seq(
        StructField("truck", StringType, nullable = false),
        StructField("_timestamp", TimestampType, nullable = false),
        StructField("channel", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("unit", StringType, nullable = false)
      ))

      import sparkSession.implicits._

      // convert to json and select only channel 'T_motTemp_Lft'
      val channels = kafka.selectExpr("cast (value as string) as json").
        select(from_json($"json", schema) as "channels").
        select("channels.*").
        where("channel == 'T_motTemp_Lft'")

      // group by 1 hour and truck and find min/max and trigger an alert if condition is meet
      val alerts = channels.
        withWatermark("_timestamp", "1 minutes"). // only keep old data for 1 minutes for late updates
        groupBy(window('_timestamp, "60 minutes") as "alert_time", 'truck).
     //   agg(collect_list(struct('_timestamp, 'value)))
       agg(increasingTemperatureAlert('_timestamp, 'value))

      alerts.printSchema()

      // just write to the console
      alerts.writeStream
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .queryName("truckAlerts")
        .outputMode("update")

        .foreach(new ForeachWriter[Row]() {
          override def open(partitionId: Long, version: Long) = true

          override def process(row: Row) {
            println(row.toString())
          }

          override def close(errorOrNull: Throwable) {}

        })
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }
}

class IncreasingTemperatureAlert(threshold: Int) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      Array(
        StructField("_timestamp", TimestampType),
        StructField("value", DoubleType)
      )
    )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    Array(
      StructField("min", DoubleType),
      StructField("alerts", ArrayType(
        StructType(
          Array(
            StructField("timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StructType(
    Array(
      StructField("alerts", ArrayType(
        StructType(
          Array(
            StructField("timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null // min
    buffer(1) = Nil // empty list of t/v
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // update the min
    //todo: think about order of data
    updateMin(buffer, input.getAs[Double](0))
    // update the buffer
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    mergeMin(buffer1, buffer2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }

  private def updateMin(buffer: MutableAggregationBuffer, min: Double): Unit = {
    // keep the minimum
    buffer(0) =
      if (buffer.isNullAt(0)) min
      else if (min > buffer.getAs[Double](0)) buffer.getAs[Double](0)
      else min
  }

  private def mergeMin(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }
    if (buffer1(0) != null) {
      buffer1(0) = if (buffer1.getAs[Double](0) > buffer2.getAs[Double](0)) buffer2.getAs[Double](0) else buffer1.getAs[Double](0)

    }
  }
}


// [[2017-01-17 03:00:00.0,2017-01-17 04:00:00.0],
//      H2X3100,
//        WrappedArray([2017-01-17 03:35:30.079,37.0])]


// [[2017-01-17 03:00:00.0,2017-01-17 04:00:00.0],
//      H2X3100,
//        WrappedArray([2017-01-17 03:35:32.079,37.0], [2017-01-17 03:35:31.079,37.0], [2017-01-17 03:35:33.079,37.0], [2017-01-17 03:35:30.079,37.0])]


private class IncreasedTempThreshold extends UserDefinedAggregateFunction {

  /*override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(Seq(StructField("value", DoubleType), StructField("value2", DoubleType)))*/


  /*


  values: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- _timestamp: timestamp (nullable = true)
 |    |    |-- value: double (nullable = true)




root
 |-- alert_time: struct (nullable = true)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- truck: string (nullable = true)


 |-- values: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- _timestamp: timestamp (nullable = true)
 |    |    |-- value: double (nullable = true)



val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("score", ArrayType(StructType(Array(
          StructField("keyword", StringType),
          StructField("point", IntegerType)
        ))))
      )
    )


    */
  override def inputSchema: StructType = StructType(
    Array(
      StructField("values", ArrayType(
        StructType(
          Array(
            StructField("_timestamp", TimestampType),
            StructField("value", DoubleType)
          )
        )
      )))
  )


  override def bufferSchema: StructType = StructType(
    StructField("product", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0) * input.getAs[Double](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)

  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}
