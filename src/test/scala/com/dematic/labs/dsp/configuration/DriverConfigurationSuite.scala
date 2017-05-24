package com.dematic.labs.dsp.configuration

import org.scalatest.FunSuite

class DriverConfigurationSuite extends FunSuite {
  test("override reference.conf properties via application.conf") {
    val config = new DriverConfiguration
    // driver properties
    assert("CumulativeCount" === config.Driver.appName)
    // spark properties
    assert("local[*]" === config.Spark.masterUrl)
  }
}
