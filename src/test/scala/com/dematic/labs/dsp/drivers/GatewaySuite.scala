package com.dematic.labs.dsp.drivers

import java.nio.file.Paths

import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration
import info.batey.kafka.unit.KafkaUnit
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

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

    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}'")

    // 3) configure the driver
    val uri = getClass.getResource("/gateway.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath)
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    Gateway.setDriverConfiguration(config)
  }

  after {
    kafkaServer.shutdown()
  }

  test("complete DSP Gateway test, push signals to kafka, Spark consumes and orders by signalType and saves to other " +
    "Kafka topics") {

  /*  // 1) start the driver asynchronously
    Future {
      // start the driver
      Gateway.main(Array[String]())
    }

    // 2) push sorter signals to kafka
    Future {
      new TestSignalProducer(kafkaServer.getKafkaConnect, "gateway", numberOfSignalsPerSignalId,
        List(100, 110), SignalType.SORTER, "gatewayProducer")
    }

    // 2) push picker signals to kafka
    Future {
      new TestSignalProducer(kafkaServer.getKafkaConnect, "gateway", numberOfSignalsPerSignalId,
        List(120, 130), SignalType.SORTER, "gatewayProducer")
    }


    Awaitility.waitAtMost(5, TimeUnit.MINUTES).untilTrue(new AtomicBoolean(false))
    print()*/
  }
}
