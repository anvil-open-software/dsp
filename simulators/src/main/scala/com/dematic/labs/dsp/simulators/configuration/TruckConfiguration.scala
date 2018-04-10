package com.dematic.labs.dsp.simulators.configuration

import java.util

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration
import com.google.common.collect.Iterables

object TruckConfiguration {

  class Builder() extends ProducerConfiguration.Builder[TruckConfiguration.Builder] {
    private val TRUCK_ID_RANGE = "producer.truckIdRange"
    private val URL = "producer.influxdb.url"
    private val DATABASE = "producer.influxdb.database"
    private val USERNAME = "producer.influxdb.username"
    private val PASSWORD = "producer.influxdb.password"
    private val PREDICATE_DATE_RANGE = "producer.influxdb.query.predicate"
    private val ANOMALIES_SEND = "producer.anomalies.send"
    private val ANOMALIES_FILTER_THRESHOLD = "producer.anomalies.filter.threshold"
    private val GAP_THRESHOLD_IN_MILLIS="producer.gap.thresholdInMilliseconds"


    val truckIdRange: util.List[Integer] = getConfig.getIntList(TRUCK_ID_RANGE)
    val url: String = getConfig.getString(URL)
    val database: String = getConfig.getString(DATABASE)
    val username: String = getConfig.getString(USERNAME)
    val password: String = getConfig.getString(PASSWORD)
    val predicateDateRange: util.List[String] = getConfig.getStringList(PREDICATE_DATE_RANGE)
    val sendAnomalies: Boolean = getConfig.getBoolean(ANOMALIES_SEND)
    val anomaliesFilterThreshhold: Int = getConfig.getInt(ANOMALIES_FILTER_THRESHOLD)
    val gapThresholdInMillis: Long = getConfig.getLong(GAP_THRESHOLD_IN_MILLIS)

    override def getThis: TruckConfiguration.Builder = {
      this
    }

    def build: TruckConfiguration = {
      new TruckConfiguration(this)
    }
  }
}

final class TruckConfiguration private[TruckConfiguration](val builder: TruckConfiguration.Builder)
  extends ProducerConfiguration(builder) {
  private val truckIdRange: util.List[Integer] = builder.truckIdRange
  private val url: String = builder.url
  private val database: String = builder.database
  private val username: String = builder.username
  private val password: String = builder.password
  private val predicateDateRange: util.List[String] = builder.predicateDateRange
  private val anomaliesSend: Boolean = builder.sendAnomalies
  private val anomaliesFilterThreshhold:Int = builder.anomaliesFilterThreshhold
  private val gapThresholdInMillis:Long = builder.gapThresholdInMillis

  def getTruckIdRange: util.List[Integer] = {
    truckIdRange
  }

  def getTruckIdRangeLow: Integer = {
    Iterables.getFirst(truckIdRange, new Integer(0))
  }

  def getTruckIdRangeHigh: Integer = {
    Iterables.getLast(truckIdRange, new Integer(0))
  }

  def getUrl: String = {
    url
  }

  def getDatabase: String = {
    database
  }

  def getUsername: String = {
    username
  }

  def getPassword: String = {
    password
  }

  def getPredicateDateRange: util.List[String] = {
    predicateDateRange
  }

  def getPredicateDateRangeLow: String = {
    Iterables.getFirst(predicateDateRange, null)
  }

  def getPredicateDateRangeHigh: String = {
    Iterables.getLast(predicateDateRange)
  }

  def getAnomaliesSend: Boolean = {
    anomaliesSend
  }

  def getAnomaliesFilterThreshhold: Int = {
    anomaliesFilterThreshhold
  }

  /**
    *
    * @return 0 if gap replay is not used, otherwise the space between two points that causes a sleep
    */
 def getGapThresholdInMillis: Long= {
   gapThresholdInMillis
 }

  override def toString: String = {
    "TruckConfiguration{} " + super.toString
  }
}
