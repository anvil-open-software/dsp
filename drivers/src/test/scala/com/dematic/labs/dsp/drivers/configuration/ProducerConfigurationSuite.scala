/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers.configuration

import com.dematic.labs.toolkit_bigdata.simulators.configuration.MinimalProducerConfiguration
import org.scalatest.FunSuite

class ProducerConfigurationSuite extends FunSuite {
  // set system property to ensure correct file used.
  System.setProperty("config.file", "src/test/resources/throughput.conf")

  test("minimal configuration test") {
    // from throughput.conf
    val config = new MinimalProducerConfiguration.Builder().build
    assert("throughput" === config.getId)
    assert(3 === config.getDurationInMinutes)
    assert("localhost:9092" === config.getBootstrapServers)
    assert("throughput" === config.getTopics)
    assert(5 === config.getRetries)
    // from reference.conf
    assert("org.apache.kafka.common.serialization.StringSerializer" === config.getKeySerializer)
    assert("org.apache.kafka.common.serialization.StringSerializer" === config.getValueSerializer)
    assert("all" === config.getAcks)
  }
}