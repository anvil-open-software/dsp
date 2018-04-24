package com.dematic.labs.dsp.drivers.configuration

import java.nio.file.Paths

import com.dematic.labs.dsp.tsdb.influxdb.{InfluxDBConnector,InfluxDBConsts}
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
    assert(config.getSparkWindowDuration === "60 minutes")
    assert(config.getSparkWindowSlideDuration === "60 minutes")
    // kafka properties
    // from persister.conf
    assert(config.getKafkaBootstrapServers === "localhost:9092")
    // from persister.conf
    assert(config.getKafkaTopics === "persister")
  }

  test("test generic config") {
    val uri = getClass.getResource("/truckToInfluxDB.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile).build
    assert(config.getConfigString(InfluxDBConsts.INFLUXDB_DATABASE) === "truckConfigTest")
    assert(config.getConfigNumber(InfluxDBConsts.INFLUXDB_BATCH_COUNT) === 5101)
    assert(config.getConfigNumber(InfluxDBConsts.INFLUXDB_BATCH_FLUSH_SECONDS) === 2)

  }
}
