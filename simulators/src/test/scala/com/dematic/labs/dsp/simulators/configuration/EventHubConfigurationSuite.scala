/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators.configuration

import org.scalatest.FunSuite

class EventHubConfigurationSuite extends FunSuite {
  // set system property to ensure correct file used.
  System.setProperty("config.resource", "rateLimitedEventHubProducer.conf")

  test("event hub configuration from conf file") {
    val config = new EventHubConfiguration.Builder().build
    // producer config
    assert(config.getId === "eventhub")
    assert(config.getSignalIdRangeLow === 100)
    assert(config.getSignalIdRangeHigh === 200)
    assert(config.getDurationInMinutes === 3)
    // eventhub config
    assert(config.getNamespace === "myNamespace")
    assert(config.getName === "myName")
    assert(config.getAccessKeyName === "mySharedAccessKeyName")
    assert(config.getAccessKey === "mySharedAccessKey")
  }
}