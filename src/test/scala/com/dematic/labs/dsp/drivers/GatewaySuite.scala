package com.dematic.labs.dsp.drivers

import java.lang
import java.nio.file.Paths
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.{Collections, Properties}

import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration
import com.dematic.labs.dsp.data.SignalType.{DMS, PICKER, SORTER}
import com.dematic.labs.dsp.simulators.TestSignalProducer
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.awaitility.Awaitility._
import org.cassandraunit.utils.EmbeddedCassandraServerHelper.cleanEmbeddedCassandra
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class GatewaySuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("GatewaySuite")

  @Rule var checkpoint = new TemporaryFolder

  private val kafkaServer = new KafkaUnit
  private val numberOfSignalsPerSignalId = 100
  private val es = Executors.newCachedThreadPool

  before {
    // create the checkpoint dir
    checkpoint.create()
    // 1) start kafka server and create topic, only one broker created during testing
    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    // create all topics
    kafkaServer.createTopic("gateway", 1)
    kafkaServer.createTopic("sorter", 1)
    kafkaServer.createTopic("picker", 1)
    kafkaServer.createTopic("dms", 1)

    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}'")

    // 3) configure the drivers
    val gateway = new DriverUnitTestConfiguration.Builder(Paths.get(getClass.getResource("/gateway.conf").toURI).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/gateway")
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .build
    // set the configuration
    Gateway.setDriverConfiguration(gateway)

    val gatewayConsumer =
      new DriverUnitTestConfiguration.Builder(Paths.get(getClass.getResource("/gatewayConsumer.conf").toURI).toFile)
        .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/gatewayconsumer")
        .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
        .build
    // set the configuration
    GatewayConsumer.setDriverConfiguration(gatewayConsumer)
  }

  after {
    try {
      kafkaServer.shutdown()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try {
      cleanEmbeddedCassandra()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try {
      es.shutdownNow()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
  }

  test("complete DSP Gateway test, push signals to kafka, Spark consumes and orders by signalType and saves to other " +
    "Kafka topics, GatewayConsumer will consume from all the signalType topics") {

    // 1) start the drivers asynchronously
    {
      es.submit(new Runnable {
        override def run() {
          Gateway.main(Array[String]())
        }
      })
      es.submit(new Runnable {
        override def run() {
          GatewayConsumer.main(Array[String]())
        }
      })
    }

    // total signals per Id and type = 3300, total by forwarded topic = 1100

    // 2) push sorter signals to kafka
    {
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
    }

    // 3) create a kafka consumer and ensure all signals by type have been forwarded to all topics
    {
      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getKafkaConnect)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "gateway_suite")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val kc: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

      // validate all messages were forwarded to topic
      validateSignalCountByTopic(kc, "sorter", 1100)
      validateSignalCountByTopic(kc, "picker", 1100)
      validateSignalCountByTopic(kc, "dms", 1100)
    }
  }

  private def validateSignalCountByTopic(kc: KafkaConsumer[String, String], topic: String, signalsPerTopic: Int) {
    // Subscribe to the topic.
    kc.subscribe(Collections.singletonList(topic))
    // keep polling until signal count is complete
    var count = 0
    await().atMost(2, TimeUnit.MINUTES).until(new Callable[lang.Boolean] {
      override def call(): lang.Boolean = {
        count = count + kc.poll(1000).count()
        count == signalsPerTopic
      }
    })
  }
}