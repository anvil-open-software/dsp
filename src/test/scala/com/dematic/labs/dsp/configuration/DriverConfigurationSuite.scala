package com.dematic.labs.dsp.configuration

import java.nio.file.Paths

import org.scalatest.FunSuite

class DriverConfigurationSuite extends FunSuite {
  test("override reference.conf properties via persister.conf") {
    val uri = getClass.getResource("/persister.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile).build
    // driver properties

    // from persister.conf
    assert(config.getDriverAppName === "persister")
    // spark properties
    // from persister.conf
    assert(config.getSparkMaster === "local[*]")
    // from persister.conf
    assert(config.getSparkCheckpointLocation === "/tmp/checkpoint")
    // from reference.conf
    assert(config.getSparkQueryTrigger === "0 seconds")
    // kafka properties
    // from persister.conf
    assert(config.getKafkaBootstrapServer === "localhost:9092")
    // from persister.conf
    assert(config.getKafkaTopics === "persister")
  }
}
