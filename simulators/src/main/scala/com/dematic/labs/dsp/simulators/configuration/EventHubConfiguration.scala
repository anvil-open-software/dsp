/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators.configuration

import com.dematic.labs.toolkit_bigdata.simulators.configuration.ProducerConfiguration

object EventHubConfiguration {
  class Builder() extends ProducerConfiguration.Builder[EventHubConfiguration.Builder] {
    private val NAMESPACE = "eventhub.namespace"
    private val NAME = "eventhub.name"
    private val ACCESS_KEY_NAME = "eventhub.sharedAccessKeyName"
    private val ACCESS_KEY = "eventhub.sharedAccessKey"

    val namespace: String = getConfig.getString(NAMESPACE)
    val name: String = getConfig.getString(NAME)
    val accessKeyName: String = getConfig.getString(ACCESS_KEY_NAME)
    val accessKey: String = getConfig.getString(ACCESS_KEY)

    override def getThis: EventHubConfiguration.Builder = {
      this
    }

    def build: EventHubConfiguration = {
      new EventHubConfiguration(this)
    }
  }
}

final class EventHubConfiguration private[EventHubConfiguration](val builder: EventHubConfiguration.Builder)
  extends ProducerConfiguration(builder) {

  private val namespace = builder.namespace
  private val name = builder.name
  private val accessKeyName = builder.accessKeyName
  private val accessKey = builder.accessKey

  def getNamespace: String = {
    namespace
  }

  def getName: String = {
    name
  }

  def getAccessKeyName: String = {
    accessKeyName
  }

  def getAccessKey: String = {
    accessKey
  }

  override def toString: String = {
    "EventHubConfiguration{} " + super.toString
  }
}