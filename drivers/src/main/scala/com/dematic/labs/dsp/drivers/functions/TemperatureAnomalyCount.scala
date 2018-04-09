package com.dematic.labs.dsp.drivers.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class TemperatureAnomalyCount extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function
  override def inputSchema: StructType = StructType(
    Array(
      StructField("value", DoubleType)
    )
  )

  // This is the internal fields you keep for computing your aggregate
  override def bufferSchema = StructType(Array(
    StructField("values", ArrayType(DoubleType)))
  )

  // define the return type
  override def dataType: DataType = IntegerType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array.empty[Double]) // array of values
  }

  // This method takes a buffer and an input row and updates the buffer.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // just update the values to be processed
    var tempArray = new ListBuffer[Double]()
    tempArray ++= buffer.getAs[List[Double]](0)
    val inputValues = input.getAs[Double](0)
    tempArray += inputValues
    buffer.update(0, tempArray)
  }

  // This method takes two buffers and combines them to produce a single buffer.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var tempArray = new ListBuffer[Double]()
    tempArray ++= buffer1.getAs[List[Double]](0)
    tempArray ++= buffer2.getAs[List[Double]](0)
    buffer1.update(0, tempArray)
  }

  // Evaluate the buffer once all the values have been merged and updated
  override def evaluate(buffer: Row): Any = {
    val values = buffer.getSeq[Double](0)
    // calculate the mean
    val mean = values.sum / values.size
    // determine the # of alerts
    var alerts = 0
    val threshold = 10 // std dev, needs to be configure
    values.foreach(value => {
      if (math.abs(mean - value) > threshold) alerts = alerts + 1
    })
    alerts
  }
}