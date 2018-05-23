package com.dematic.labs.dsp.drivers

import java.lang
import java.nio.file.Paths
import java.util.concurrent.{Callable, TimeUnit}
import java.util.{Collections, Properties}

import com.dematic.labs.dsp.drivers.configuration.DriverUnitTestConfiguration
import com.dematic.labs.dsp.drivers.trucks.StatefulTruckAlerts
import com.dematic.labs.dsp.simulators.TestTruckProducer
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.awaitility.Awaitility.await
import org.joda.time.DateTime
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ConcurrentTask}
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

class CompleteStatefulTruckAlertsSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("CompleteStatefulTruckAlertsSuite")

  @Rule var checkpoint = new TemporaryFolder

  private val kafkaServer = new KafkaUnit
  private var kc: KafkaConsumer[String, String] = _
  private var task: ConcurrentTask[Unit] = _

  before {
    // 1) start kafka server and create topic, only one broker created during testing

    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    kafkaServer.createTopic("linde", 1)
    kafkaServer.createTopic("alerts", 1)

    // 2) create the kafka consumer used to retrieve the alerts

    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getKafkaConnect)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "StatefulTruckAlertsSuite")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kc = new KafkaConsumer[String, String](props)
    // Subscribe to the topic.
    kc.subscribe(Collections.singletonList("alerts"))

    // 3) configure the driver

    checkpoint.create() // create the checkpoint driver
    val uri = getClass.getResource("/truckAlerts.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/truckAlerts")
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    StatefulTruckAlerts.setDriverConfiguration(config)
    // start the driver
    task = ConcurrentTask {
      StatefulTruckAlerts.main(Array[String]())
    }
  }

  test("complete DSP StatefulTruckAlert test, send T_motTemp_Lft json messages to kafka, Spark consumes " +
    "messages and creates alerts based on the temperature value rising more then 10 degrees, then send alerts to " +
    "another kafka topic.") {

    // timestamp used for testing
    val timestamp = DateTime.now()

    // 1) push truck messages to kafka
    {
      ConcurrentTask {
        // create a list of json messages to send
        val jsonMessages: List[String] = List(
          s"""{"truck":"H2X3501117","_timestamp":"$timestamp","channel":"T_motTemp_Lft","value":5.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(1)}","channel":"T_motTemp_Lft","value":10.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(2)}","channel":"T_motTemp_Lft","value":15.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(3)}","channel":"T_motTemp_Lft","value":20.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(4)}","channel":"T_motTemp_Lft","value":25.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(5)}","channel":"T_motTemp_Lft","value":30.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(6)}","channel":"T_motTemp_Lft","value":35.0}"""
        )
        new TestTruckProducer(kafkaServer.getKafkaConnect, "linde", jsonMessages)
      }
    }

    // 2) poll until alert json is in the output sink topic
    var totalCount = 0
    // keep polling until alerts are generated
    await().atMost(2, TimeUnit.MINUTES).until(new Callable[lang.Boolean] {
      override def call(): lang.Boolean = {
        val consumerRecord = kc.poll(1000)
        // for now just test count, 2 alerts should have been generated, need to test all use cases and variations
        // on data
        totalCount = totalCount + consumerRecord.count
        if (totalCount == 2) return true
        false // 2 alerts should have been created based on the json sent to the kafka topic
      }
    })
  }

  after {
    try
      kafkaServer.shutdown()
    catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try
      // catch exceptions from the driver
      task.onComplete {
        case Success(s) => logger.info(s.toString)
        case Failure(e) => logger.error(e.toString)
      }
    catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
  }
}
