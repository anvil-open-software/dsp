package com.dematic.labs.dsp.configuration

import org.scalatest.FunSuite

class DriverConfigurationSuite extends FunSuite {
  // set system property to ensure correct file used.
  System.setProperty("config.resource", "application.conf")

  test("override reference.conf properties via application.conf") {
    // driver properties
    //todo: fix properties, need to load test properties
   /* // from application.conf
    assert("CumulativeCount" === DriverConfiguration.Driver.appName)
    // spark properties
    // from application.conf
    assert("local[*]" === DriverConfiguration.Spark.masterUrl)
    // from application.conf
    assert("/tmp/checkpoint" === DriverConfiguration.Spark.checkpointLocation)
    // from reference.conf
    assert("0 seconds" === DriverConfiguration.Spark.queryTrigger)
    // kafka properties
    // from application.conf
    assert("localhost:9092" === DriverConfiguration.Kafka.bootstrapServers)
    // from application.conf
    assert("test" === DriverConfiguration.Kafka.topics)*/
  }
}
