package com.dematic.labs.dsp.drivers

import java.nio.file.Paths

import com.dematic.labs.dsp.drivers.configuration.DriverUnitTestConfiguration
import info.batey.kafka.unit.KafkaUnit
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.language.reflectiveCalls

class TruckAlertsSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("TruckAlertsSuite")

  @Rule var checkpoint = new TemporaryFolder

  private val kafkaServer = new KafkaUnit
  private val topicAndKeyspace = "truckAlerts"

  before {
    // create the checkpoint dir
    checkpoint.create()
    // 1) start kafka server and create topic, only one broker created during testing
    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    kafkaServer.createTopic(topicAndKeyspace, 1)
    // 2) configure the driver
    val uri = getClass.getResource("/truckAlerts.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/truckAlerts")
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    TruckAlerts.setDriverConfiguration(config)
  }

  after {
    try {
      kafkaServer.shutdown()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }

  }
}