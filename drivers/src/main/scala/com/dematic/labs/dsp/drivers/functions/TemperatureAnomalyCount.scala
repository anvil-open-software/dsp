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
      if (math.abs(mean - value) >= threshold) count = count + 1
    }
    count
  }
}