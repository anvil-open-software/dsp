package com.dematic.labs.dsp.drivers

import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import com.dematic.labs.dsp.drivers.configuration.{DefaultDriverConfiguration, DriverConfiguration}
import com.google.common.base.Strings
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

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

    val temperatureAnomalyCount = new TemperatureAnomalyCount

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
        agg(mean('value) as "mean", temperatureAnomalyCount('value) as "alerts",
          collect_list(struct('_timestamp, 'value)) as "values").where('alerts > 0)

      // just write to the console
      alerts.writeStream
        .format("console")
        .trigger(ProcessingTime(config.getSparkQueryTrigger))
        .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation)
        .option("truncate", "false")
        .queryName("truckAlerts")
        .outputMode("update")
        .start

      // keep alive
      sparkSession.streams.awaitAnyTermination
    } finally
      sparkSession.close
  }

  class TemperatureAnomalyCount extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function
    override def inputSchema: StructType = StructType(
      Array(
        StructField("value", DoubleType)
      )
    )

    // This is the internal fields you keep for computing your aggregate
    override def bufferSchema = StructType(
      Array(
        StructField("values", ArrayType(DoubleType)),
        StructField("mean", StructType(
          Array(
            StructField("sum", DoubleType),
            StructField("count", IntegerType))
        )))
    )

    // define the return type
    override def dataType: DataType = IntegerType

    // Does the function return the same value for the same input?
    override def deterministic: Boolean = true

    // Initial values
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Array.empty[Double]) // temperature values
      buffer.update(1, (0.0, 0)) // sum, value count
    }

    // Updated based on Input
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      // only need to update the values not the sum/count
      var tempArray = new ListBuffer[Double]()
      tempArray ++= buffer.getAs[List[Double]](0)
      val inputValues = input.getAs[Double](0)
      tempArray += inputValues
      buffer.update(0, tempArray)
    }

    // Merge two schemas
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var tempArray = new ListBuffer[Double]()
      tempArray ++= buffer1.getAs[List[Double]](0)
      tempArray ++= buffer2.getAs[List[Double]](0)
      buffer1.update(0, tempArray)
      // merge the sum/count, tempArray has already been merged
      val sum = tempArray.sum
      val count = tempArray.length
      buffer1.update(1, (sum, count))
    }

    // Output
    override def evaluate(buffer: Row): Any = {
      var count = 0
      val struct = buffer.getStruct(1)
      val mean = struct.getDouble(0) / struct.getInt(1)
      val threshold = 10 // std dev, needs to be configure

      import scala.collection.JavaConversions._
      for (value <- buffer.getList[Double](0)) {
        if (math.abs(mean - value) >= threshold) count += 1
      }
      count
    }
  }
}