package com.dematic.labs.dsp.data

import scala.beans.BeanProperty

/**
  * Used mostly for testing, makes it easier to get values from json.
  *
  * @param processing_time -- processing time from spark
  * @param alert_time      -- time the alerts fell into
  * @param truck           -- truck id
  * @param alerts          -- number of alerts
  * @param values          -- list of timestamp/values that created the alert
  */
class Alert(@BeanProperty val processing_time: String, @BeanProperty val alert_time: AlertTime,
            @BeanProperty val truck: String, @BeanProperty val alerts: Int,
            @BeanProperty val values: List[AlertValues]) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[Alert]

  override def equals(other: Any): Boolean = other match {
    case that: Alert =>
      (that canEqual this) &&
        processing_time == that.processing_time &&
        alert_time == that.alert_time &&
        truck == that.truck &&
        alerts == that.alerts &&
        values == that.values
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(processing_time, alert_time, truck, alerts, values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Alert($processing_time, $alert_time, $truck, $alerts, $values)"
}

class AlertTime(@BeanProperty val start: String, @BeanProperty val end: String) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[AlertTime]

  override def equals(other: Any): Boolean = other match {
    case that: AlertTime =>
      (that canEqual this) &&
        start == that.start &&
        end == that.end
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(start, end)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"AlertTime($start, $end)"
}

class AlertValues(val _timestamp: String, @BeanProperty val value: String) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[AlertValues]

  override def equals(other: Any): Boolean = other match {
    case that: AlertValues =>
      (that canEqual this) &&
        _timestamp == that._timestamp &&
        value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(_timestamp, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"AlertValues($_timestamp, $value)"
}