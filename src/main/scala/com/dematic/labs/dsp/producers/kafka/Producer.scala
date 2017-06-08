package com.dematic.labs.dsp.producers.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.configuration.DriverConfiguration._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.Promise

class Producer {
  private val kafkaProps = new Properties()
  // required configuration
  kafkaProps.put("bootstrap.servers", Kafka.bootstrapServers)
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("acks", "all")
  kafkaProps.put("producerType", "sync")
  kafkaProps.put("retries", "3")

  // connection to Kafka!
  private val producer = new KafkaProducer[String, String](kafkaProps)

  def send(value: String) {
    if ("producerType" == "sync") sendSync(value) else sendAsync(value)
  }

  def sendSync(value: String) {
    val record = new ProducerRecord[String, String](Kafka.topics, value)
    try {
      producer.send(record).get()
    } catch {
      case e: Exception =>
        println(s"Unexpected Error\n: ${e.printStackTrace()}")
    }
  }

  def sendAsync(value: String) {
    val record = new ProducerRecord[String, String](Kafka.topics, value)
    val p = Promise[(RecordMetadata, Exception)]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) {
        p.success((metadata, exception))
      }
    })
  }

  // will wait for 30 seconds, to ensure all msgs are sent, if there are still pending msgs, they will be dropped
  def close() {
    producer.flush()
    producer.close(30, TimeUnit.SECONDS)
  }
}