/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators.configuration

import java.util

import com.dematic.labs.dsp.simulators.configuration.PartitionStrategy.PartitionStrategy
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

    private val TRUCKS_PER_THREAD = "producer.trucksPerThread"

    private val ANOMALIES_SEND = "producer.anomalies.send"
    private val ANOMALIES_FILTER_THRESHOLD = "producer.anomalies.filter.threshold"

    private val GAP_THRESHOLD_IN_MILLIS="producer.gap.thresholdInMilliseconds"
    private val PRODUCER_PARTITION_STRATEGY = "producer.partition.strategy"
    private val PRODUCER_RATE_LIMITER_PERMITS_PER_SECOND = "producer.rateLimiter.permitsPerSecond"


    val truckIdRange: util.List[Integer] = getConfig.getIntList(TRUCK_ID_RANGE)
    val url: String = getConfig.getString(URL)
    val database: String = getConfig.getString(DATABASE)
    val username: String = getConfig.getString(USERNAME)
    val password: String = getConfig.getString(PASSWORD)
    val predicateDateRange: util.List[String] = getConfig.getStringList(PREDICATE_DATE_RANGE)
    val trucksPerThread: Int =  getConfig.getInt(TRUCKS_PER_THREAD)
    val sendAnomalies: Boolean = getConfig.getBoolean(ANOMALIES_SEND)
    val anomaliesFilterThreshhold: Int = getConfig.getInt(ANOMALIES_FILTER_THRESHOLD)
    val gapThresholdInMillis: Long = getConfig.getLong(GAP_THRESHOLD_IN_MILLIS)
    val partitionStrategy: PartitionStrategy = PartitionStrategy.withName(getConfig.getString(PRODUCER_PARTITION_STRATEGY))
    val rateLimiterPermitsPerSecond: Double = getConfig.getInt(PRODUCER_RATE_LIMITER_PERMITS_PER_SECOND)

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
  private val trucksPerThread: Int = builder.trucksPerThread
  private val anomaliesSend: Boolean = builder.sendAnomalies
  private val anomaliesFilterThreshhold:Int = builder.anomaliesFilterThreshhold
  private val gapThresholdInMillis:Long = builder.gapThresholdInMillis
  private val partitionStrategy: PartitionStrategy = builder.partitionStrategy
  private val rateLimiterPermitsPerSecond:Double = builder.rateLimiterPermitsPerSecond


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

  def getTrucksPerThread: Int = {
    trucksPerThread
  }

  def getPartitionStrategy: PartitionStrategy = {
    partitionStrategy
  }
  def getAnomaliesSend: Boolean = {
    anomaliesSend
  }

  def getAnomaliesFilterThreshhold: Int = {
    anomaliesFilterThreshhold
  }

  def getRateLimiterPermitsPerSecond: Double = {
    rateLimiterPermitsPerSecond
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
