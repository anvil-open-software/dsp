package com.dematic.labs.dsp.drivers

import java.lang
import java.nio.file.Paths
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.{Collections, Properties}

import com.dematic.labs.dsp.drivers.configuration.DriverUnitTestConfiguration
import com.dematic.labs.dsp.drivers.trucks.TruckAlerts
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.awaitility.Awaitility.await
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.reflectiveCalls

class TruckAlertsSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("TruckAlertsSuite")

  @Rule var checkpoint = new TemporaryFolder

  private val kafkaServer = new KafkaUnit
  private val es = Executors.newCachedThreadPool

  before {
    // create the checkpoint dir
    checkpoint.create()
    // 1) start kafka server and create topic, only one broker created during testing
    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    kafkaServer.createTopic("linde", 1)
    kafkaServer.createTopic("alerts", 1)
    // 2) configure the driver
    val uri = getClass.getResource("/truckAlerts.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/truckAlerts")
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    TruckAlerts.setDriverConfiguration(config)
  }

  test("complete DSP TruckAlert test, send T_motTemp_Lft json messages to kafka, Spark consumes messages " +
    "and creates alerts based on the temperature value rising more then 10 degrees, then send alerts to another " +
    "kafka topic.") {

    // 1) start the driver asynchronously
    {
      es.submit(new Runnable {
        override def run() {
          TruckAlerts.main(Array[String]())
        }
      })
    }

    // 2) push sorter signals to kafka
    {
      Future {

      }
    }

    // 3) create a kafka consumer and ensure alerts were created and sent to kafka alert topic
    {
      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getKafkaConnect)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "TruckAlertsSuite")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val kc: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

      // Subscribe to the topic.
      kc.subscribe(Collections.singletonList("alerts"))
      // keep polling until signal count is complete
      var alerts = 0
      await().atMost(2, TimeUnit.MINUTES).until(new Callable[lang.Boolean] {
        override def call(): lang.Boolean = {
          alerts = alerts + kc.poll(1000).count()
          alerts == 2 // 2 alerts should have been created based on the json sent to the kafka topic
        }
      })
    }
  }

  after {
    try {
      kafkaServer.shutdown()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try {
      es.shutdownNow()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
  }
}