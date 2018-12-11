/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators

import java.nio.charset.Charset
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class TestTruckProducer(val bootstrapServer: String, val topic: String, val jsonMessages: List[String]) {
  private val properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
  properties.put(ProducerConfig.ACKS_CONFIG, "all")

  private val producer = new KafkaProducer[String, AnyRef](properties)

  try {
    // cycle through and push all json messages
    jsonMessages.foreach(json => {
      producer.send(new ProducerRecord[String, AnyRef](topic, json.getBytes(Charset.defaultCharset())))
    })
  } finally {
    producer.close()
  }
}
