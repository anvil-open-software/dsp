package com.dematic.labs.dsp.drivers.functions

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class TemperatureAnomalyCount extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function
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
      StructField("alerts", ArrayType(
        StructType(
          Array(
            StructField("_timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  // define the return type
  override def dataType: DataType = IntegerType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array.empty[(Timestamp, Double)]) // empty list of t/v
  }

  // This method takes a buffer and an input row and updates the buffer.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // internal structure is min, t/v
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer.getAs[List[(Timestamp, Double)]](0)
    val inputValues: (Timestamp, Double) = (input.getTimestamp(0), input.getDouble(1))
    tempArray += inputValues
    buffer.update(0, tempArray)
  }

  // This method takes two buffers and combines them to produce a single buffer.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer1.getAs[List[(Timestamp, Double)]](0)
    tempArray ++= buffer2.getAs[List[(Timestamp, Double)]](0)
    buffer1.update(0, tempArray)
  }

  //noinspection ConvertExpressionToSAM
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  // Evaluate the buffer once all the values have been merged and updated
  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    import scala.collection.JavaConversions._
    val sortedValues = buffer.getList[Row](0) sortBy (time => time.getTimestamp(0))
    // values are sorted by time, min is the first value
    var min = sortedValues.get(0).getDouble(1)
    var alerts = 0
    sortedValues.foreach(tuple => {
      val value = tuple.getDouble(1)
      if (math.abs(value - min) > 10) {
        alerts = alerts + 1
        // reset the min
        min = value
      }
      // reset the min if needed
      if (value < min) min = value
    })
    alerts
  }
}