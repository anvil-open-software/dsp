package com.dematic.labs.dsp.configuration

import org.scalatest.FunSuite

class DriverConfigurationSuite extends FunSuite {
  test("override reference.conf properties via application.conf") {
    val config = new DriverConfiguration
    // driver properties
    // from application.conf
    assert("CumulativeCount" === config.Driver.appName)
    // spark properties
    // from application.conf
    assert("local[*]" === config.Spark.masterUrl)
    // from reference.conf
    assert("0 seconds" === config.Spark.queryTrigger)
    // kafka properties
    // from application.conf
    assert("localhost:9092" === config.Kafka.bootstrapServers)
    // from application.conf
    assert("test" === config.Kafka.topics)
  }
}
