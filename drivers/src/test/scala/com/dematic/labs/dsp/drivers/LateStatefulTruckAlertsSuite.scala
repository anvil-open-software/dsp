/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.drivers

import java.lang
import java.nio.file.Paths
import java.util.concurrent.{Callable, TimeUnit}
import java.util.{Collections, Properties}

import com.dematic.labs.dsp.data.StatefulAlert
import com.dematic.labs.dsp.data.Utils.fromJson
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

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ConcurrentTask}
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

class LateStatefulTruckAlertsSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("LateStatefulTruckAlertsSuite")

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

  test("complete DSP StatefulTruckAlert test, send truck json message that is more then an hour late at " +
    "startup and continues stream") {

    // start up should calculate all alerts, continues should filter out data that is > then 1 hour from previous message

    // timestamp used for testing
    val timestamp = DateTime.now()
    // used for collecting alerts and validating data
    val alerts: ListBuffer[StatefulAlert] = ListBuffer()

    // 1) push truck messages to kafka on startup, will calculate 2 alerts
    {
      ConcurrentTask {
        // create a list of json messages to send
        val jsonMessages: List[String] = List(
          s"""{"truck":"H2X3501117","_timestamp":"$timestamp","channel":"T_motTemp_Lft","value":5.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusSeconds(1)}","channel":"T_motTemp_Lft","value":20.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusMinutes(61)}","channel":"T_motTemp_Lft","value":50.0}""",
          s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusMinutes(62)}","channel":"T_motTemp_Lft","value":70.0}"""
        )
        new TestTruckProducer(kafkaServer.getKafkaConnect, "linde", jsonMessages)
      }

      // 2) poll until alert json is in the output sink topic
      await().atMost(2, TimeUnit.MINUTES).until(new Callable[lang.Boolean] {
        override def call(): lang.Boolean = {
          val consumerRecord = kc.poll(1000)
          // check for alerts with specific min and max
          consumerRecord.foreach(record => {
            val json: String = record.value()
            // turn into an alert object to make it easier to get the # of alerts generated
            alerts += fromJson[StatefulAlert](json)
          })
          // just check that 2 alerts were created
          if (alerts.size == 2) return true
          false
        }
      })
    }

    // 3) push another truck message to kafka and ensure an alert was not created
    ConcurrentTask {
      // create a list of json messages to send
      val jsonMessages: List[String] = List(
        s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusMinutes(65)}","channel":"T_motTemp_Lft","value":100.0}""",
        s"""{"truck":"H2X3501117","_timestamp":"${timestamp.plusMinutes(130)}","channel":"T_motTemp_Lft","value":200.0}"""
      )
      new TestTruckProducer(kafkaServer.getKafkaConnect, "linde", jsonMessages)
    }

    // 4) poll kafka and ensure only the first 2 alerts exist plus 1 new one
    await().atMost(2, TimeUnit.MINUTES).until(new Callable[lang.Boolean] {
      override def call(): lang.Boolean = {
        val consumerRecord = kc.poll(1000)
        // check for alerts with specific min and max
        consumerRecord.foreach(record => {
          val json: String = record.value()
          // turn into an alert object to make it easier to get the # of alerts generated
          alerts += fromJson[StatefulAlert](json)
        })
        // one more alert should have been created
        if (alerts.size == 3 && alerts.last.min.value.toDouble == 70 && alerts.last.max.value.toDouble == 100) return true
        false
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
