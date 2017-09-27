package com.dematic.labs.dsp.drivers

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration
import com.dematic.labs.dsp.data.SignalType.{DMS, PICKER, SORTER}
import com.dematic.labs.dsp.simulators.TestSignalProducer
import com.jayway.awaitility.Awaitility
import info.batey.kafka.unit.KafkaUnit
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class GatewaySuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("GatewaySuite")

  @Rule var checkpoint = new TemporaryFolder

  val kafkaServer = new KafkaUnit

  val numberOfSignalsPerSignalId = 100

  before {
    // create the checkpoint dir
    checkpoint.create()
    // 1) start kafka server and create topic, only one broker created during testing
    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    kafkaServer.createTopic("gateway", 1)
    kafkaServer.createTopic("sorter", 1)
    kafkaServer.createTopic("picker", 1)
    kafkaServer.createTopic("dms", 1)

    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}'")

    // 3) configure the drivers
    val gateway = new DriverUnitTestConfiguration.Builder(Paths.get(getClass.getResource("/gateway.conf").toURI).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath)
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    Gateway.setDriverConfiguration(gateway)

    val gatewayConsumer =
      new DriverUnitTestConfiguration.Builder(Paths.get(getClass.getResource("/gatewayConsumer.conf").toURI).toFile)
        .sparkCheckpointLocation(checkpoint.getRoot.getPath)
        .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
        .build
    // set the configuration
    GatewayConsumer.setDriverConfiguration(gatewayConsumer)
  }

  after {
    kafkaServer.shutdown()
  }

  test("complete DSP Gateway test, push signals to kafka, Spark consumes and orders by signalType and saves to other " +
    "Kafka topics") {

    // 1) start the drivers asynchronously
    Future {
      // start the driver
      Gateway.main(Array[String]())
    }

    Future {
      // start the driver
      GatewayConsumer.main(Array[String]())
    }

    // 2) push sorter signals to kafka
    Future {
      new TestSignalProducer(kafkaServer.getKafkaConnect, "gateway", numberOfSignalsPerSignalId,
        List(100, 110), SORTER, "gatewayProducer")
    }

    // 3) push picker signals to kafka
    Future {
      new TestSignalProducer(kafkaServer.getKafkaConnect, "gateway", numberOfSignalsPerSignalId,
        List(120, 130), PICKER, "gatewayProducer")
    }

    // 3) push dms signals to kafka
    Future {
      new TestSignalProducer(kafkaServer.getKafkaConnect, "gateway", numberOfSignalsPerSignalId,
        List(140, 150), DMS, "gatewayProducer")
    }


    Awaitility.waitAtMost(5, TimeUnit.MINUTES).untilTrue(new AtomicBoolean(false))
  }
}
