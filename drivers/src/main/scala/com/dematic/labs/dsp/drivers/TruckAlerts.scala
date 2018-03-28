package com.dematic.labs.dsp.drivers

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.collection.mutable.ListBuffer

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

    val alertCount = new AlertCount

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
        agg(alertCount('value) as "alerts", collect_list(struct('_timestamp, 'value)) as "values").where('alerts > 0)

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

  class AlertCount extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: StructType = StructType(
      Array(
        StructField("value", DoubleType)
      )
    )

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema = StructType(
      Array(
        StructField("values", ArrayType(DoubleType))
      )
    )

    // define the return type
    override def dataType: DataType = IntegerType

    // Does the function return the same value for the same input?
    override def deterministic: Boolean = true

    // Initial values
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Array.empty[Double])
    }

    // Updated based on Input
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        var tempArray = new ListBuffer[Double]()
        tempArray ++= buffer.getAs[List[Double]](0)
        val inputValues = input.getAs[Double](0)
        tempArray += inputValues
        buffer.update(0, tempArray)
      }
    }

    // Merge two schemas
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var tempArray = new ListBuffer[Double]()
      tempArray ++= buffer1.getAs[List[Double]](0)
      tempArray ++= buffer2.getAs[List[Double]](0)
      buffer1.update(0, tempArray)
    }

    // Output
    override def evaluate(buffer: Row): Any = {
      var count = 0
      val mean = 17.5 //todo try to pass in
      val threshold = 10 // std dev, needs to be configure

      import scala.collection.JavaConversions._
      for (value <- buffer.getList[Double](0)) {
        if (math.abs(mean - value) >= threshold) count = count + 1
      }
      count
    }
  }
}