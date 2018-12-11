/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers.functions

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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
            StructField("_timestamp", TimestampType),
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
            StructField("_timestamp", TimestampType),
            StructField("value", DoubleType))
        ))
      )
    )
  )

  /* override def dataType: DataType = new StructType()
     .add("alerts", ArrayType(
       new StructType()
         .add("alert", ArrayType(
           new StructType()
             .add("_timestamp", TimestampType)
             .add("value", DoubleType)
         )))
     )*/


  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null // min
    buffer.update(1, Array.empty[(Timestamp, Double)]) // empty list of t/v
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // update the min
    updateMin(buffer, input)
    // update the buffer
    updateBuffer(buffer, input)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    mergeMin(buffer1, buffer2)
    mergeBuffer(buffer1, buffer2)
  }

  //noinspection ConvertExpressionToSAM
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    var minTemp = buffer.getDouble(0)
    val sortedValues = buffer.getList[GenericRowWithSchema](1) sortBy (time => time.getTimestamp(0))
    // create the list of alerts and data
    val alerts = new ListBuffer[List[(Timestamp, Double)]]()
    // keep track of where we are
    var alertArrayIndex = 0
    var valueIndex = 1
    sortedValues.foreach(value => {
      val currentTemp = value.getDouble(1)
      if ((currentTemp - minTemp) >= threshold) {
        // update the minTemp
        minTemp = currentTemp
        // convert and add all values to the alert array
        val tmpBuffer = ListBuffer[(Timestamp, Double)]()
        sortedValues.take(valueIndex).foreach(t => {
          tmpBuffer += Tuple2(t.getTimestamp(0), t.getDouble(1))
        })
        alerts.insert(alertArrayIndex, tmpBuffer.toList)
        alertArrayIndex = alertArrayIndex + 1
      }
      valueIndex = valueIndex + 1
    })
    alerts
  }

  private def updateMin(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // keep the minimum
    val inputMin = input.getDouble(1) // input row is t/v
    // check to see if min has been set, internal structure is min, t/v
    buffer(0) =
      if (buffer.isNullAt(0)) inputMin
      else if (inputMin > buffer.getDouble(0)) buffer.getDouble(0)
      else inputMin
  }

  private def mergeMin(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // internal structure is min, t/v
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }
    if (buffer1(0) != null) {
      buffer1(0) = if (buffer1.getDouble(0) > buffer2.getDouble(0)) buffer2.getDouble(0) else buffer1.getDouble(0)
    }
  }

  private def updateBuffer(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // internal structure is min, t/v
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer.getAs[List[(Timestamp, Double)]](1)
    val inputValues: (Timestamp, Double) = (input.getTimestamp(0), input.getDouble(1))
    tempArray += inputValues
    buffer.update(1, tempArray)
  }

  private def mergeBuffer(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var tempArray = new ListBuffer[(Timestamp, Double)]()
    tempArray ++= buffer1.getAs[List[(Timestamp, Double)]](1)
    tempArray ++= buffer2.getAs[List[(Timestamp, Double)]](1)
    buffer1.update(1, tempArray)
  }
}

