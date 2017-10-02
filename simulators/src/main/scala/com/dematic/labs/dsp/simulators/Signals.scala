package com.dematic.labs.dsp.simulators

import java.time.Instant
import java.util

import com.dematic.labs.dsp.data.Signal
import com.dematic.labs.toolkit_bigdata.simulators.configuration.MinimalProducerConfiguration
import com.dematic.labs.dsp.data.SignalType._
import com.dematic.labs.dsp.data.Utils.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

// will use the reference.conf file, override kafka.bootstrap.servers, kafka.topics, and producer.id using system
// properties. Pass in the number of signals to be sent to Kafka.
object Signals extends App {
  // load all the configuration
  private val config = new MinimalProducerConfiguration.Builder().build

  // generated values
  private val nextRandomValue = {
    val random = new Random
    () => {
      val num = random.nextInt()
      if (num < 0) num * -1 else num
    }
  }

  // configure and create kafka producer
  private val properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializer)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializer)
  properties.put(ProducerConfig.ACKS_CONFIG, config.getAcks)
  properties.put(ProducerConfig.RETRIES_CONFIG, Predef.int2Integer(config.getRetries))

  private val producer = new KafkaProducer[String, AnyRef](properties)

  try {
    // cycle through the range
    val lowSignalRange: Int = config.getSignalIdRangeLow
    val highSignalRange: Int = config.getSignalIdRangeHigh
    // number of signals to send
    val numberOfSignals = args(0).toInt

    for (signalId <- lowSignalRange to highSignalRange) {
      for (_ <- 1 to numberOfSignals) {
        val json = toJson(new Signal(signalId, Instant.now.toString, SORTER.toString, nextRandomValue(),
          config.getId))
        producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json))
      }
    }
  } finally {
    producer.close()
  }
}
