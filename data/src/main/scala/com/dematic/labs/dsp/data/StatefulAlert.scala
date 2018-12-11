/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.data

import scala.beans.BeanProperty

class StatefulAlert(@BeanProperty val truck: String, @BeanProperty val min: Measurements,
                    @BeanProperty val max: Measurements, @BeanProperty val measurements: List[Measurements]) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[StatefulAlert]

  override def equals(other: Any): Boolean = other match {
    case that: StatefulAlert =>
      (that canEqual this) &&
        truck == that.truck &&
        min == that.min &&
        max == that.max &&
        measurements == that.measurements
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(truck, min, max, measurements)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"StatefulAlert($truck, $min, $max, $measurements)"
}

class Measurements(@BeanProperty val timestamp: String, @BeanProperty val value: String) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[Measurements]

  override def equals(other: Any): Boolean = other match {
    case that: Measurements =>
      (that canEqual this) &&
        timestamp == that.timestamp &&
        value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(timestamp, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Measurements($timestamp, $value)"
}
