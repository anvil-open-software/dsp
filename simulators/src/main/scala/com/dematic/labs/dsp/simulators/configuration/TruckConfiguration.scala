package com.dematic.labs.dsp.simulators.configuration

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration

object TruckConfiguration {
  class Builder() extends ProducerConfiguration.Builder[TruckConfiguration.Builder] {
    private val URL = "influxdb.url"
    private val DATABASE = "influxdb.database"
    private val USERNAME = "influxdb.username"
    private val PASSWORD = "influxdb.password"

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

  private val url = builder.url
  private val database = builder.database
  private val username = builder.username
  private val password = builder.password

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
