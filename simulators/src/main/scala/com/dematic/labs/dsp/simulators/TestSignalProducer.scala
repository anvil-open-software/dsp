/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators

import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util

import com.dematic.labs.dsp.data.Signal
import com.dematic.labs.dsp.data.SignalType.SignalType
import com.dematic.labs.dsp.data.Utils.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Used in unit test to help send signals to kafka.
  *
  * @param bootstrapServer      -- server address
  * @param topic                -- kafka topic
  * @param numberOfSignalsPerId -- number of signals per id to send
  * @param signalIdRange        -- signal Id range
  * @param id                   -- test id
  */
class TestSignalProducer(val bootstrapServer: String, val topic: String, val numberOfSignalsPerId: Int,
                         val signalIdRange: Seq[Int], val signalType: SignalType, id: String) {
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
    // cycle through the range
    val lowSignalRange: Int = signalIdRange.head
    val highSignalRange: Int = signalIdRange.last

    for (signalId <- lowSignalRange to highSignalRange) {
      // number of signals to send
      var now = Instant.now
      for (_ <- 1 to numberOfSignalsPerId) {
        val json = toJson(new Signal(signalId, now.toString, signalType.toString, nextRandomValue(), id))
        producer.send(new ProducerRecord[String, AnyRef](topic, json))
        now = now.plus(1, SECONDS)
      }
    }
  } finally {
    producer.close()
  }
}