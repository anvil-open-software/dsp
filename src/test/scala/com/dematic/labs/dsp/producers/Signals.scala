package com.dematic.labs.dsp.producers

import java.time.Instant
import java.util

import com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data.Signal
import com.dematic.labs.toolkit_bigdata.simulators.diagnostics.data.Utils.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

class Signals(val bootstrapServer: String, val topic: String, val numberOfSignals: Int, val id: String) {
  private val properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.ACKS_CONFIG, "all")

  // generated values
  private val nextRandomValue = {
    val random = new Random
    () => {
      val num = random.nextInt()
      if (num < 0) num * -1 else num
    }
  }

  private val producer = new KafkaProducer[String, AnyRef](properties)

  try {
    // number of signals to send
    for (i <- 1 to numberOfSignals) {
      val json = toJson(new Signal(i, Instant.now.toString, nextRandomValue(), id))
      producer.send(new ProducerRecord[String, AnyRef](topic, json))
    }
  } finally {
    producer.close()
  }
}

