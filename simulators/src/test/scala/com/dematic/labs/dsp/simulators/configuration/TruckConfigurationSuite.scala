package com.dematic.labs.dsp.simulators.configuration

import org.scalatest.FunSuite

class TruckConfigurationSuite extends FunSuite {
  // set system property to ensure correct file used.
  System.setProperty("config.resource", "trucks.conf")

  test("truck configuration defaults from resource built with config") {
    val config = new TruckConfiguration.Builder().build
    // producer config
    assert(config.getPartitionStrategy === PartitionStrategy.DEFAULT_KEYED_PARTITION)
  }
}