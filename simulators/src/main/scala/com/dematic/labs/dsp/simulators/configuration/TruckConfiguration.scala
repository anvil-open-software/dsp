package com.dematic.labs.dsp.simulators.configuration

import java.util

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration
import com.google.common.collect.Iterables

object TruckConfiguration {

  class Builder() extends ProducerConfiguration.Builder[TruckConfiguration.Builder] {
    private var TRUCK_ID_RANGE = "producer.truckIdRange"
    private val URL = "producer.influxdb.url"
    private val DATABASE = "producer.influxdb.database"
    private val USERNAME = "producer.influxdb.username"
    private val PASSWORD = "producer.influxdb.password"

    val truckIdRange: util.List[Integer] = getConfig.getIntList(TRUCK_ID_RANGE)
    val url: String = getConfig.getString(URL)
    val database: String = getConfig.getString(DATABASE)
    val username: String = getConfig.getString(USERNAME)
    val password: String = getConfig.getString(PASSWORD)

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
  private val truckIdRange = builder.truckIdRange
  private val url = builder.url
  private val database = builder.database
  private val username = builder.username
  private val password = builder.password

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

  override def toString: String = {
    "TruckConfiguration{} " + super.toString
  }
}
