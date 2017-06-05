package com.dematic.labs.dsp.producers.kafka

import java.util.Properties

import com.dematic.labs.dsp.configuration.DriverConfiguration
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.Promise

case class Producer(configuration: DriverConfiguration) {
  private val kafkaProps = new Properties()
  // required configuration
  kafkaProps.put("bootstrap.servers", configuration.Kafka.bootstrapServers)
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("producerType", "sync")
  kafkaProps.put("retries", "3")

  // connection to Kafka!
  private val producer = new KafkaProducer[String, String](kafkaProps)

  def send(value: String) {
    if ("producerType" == "sync") sendSync(value) else sendAsync(value)
  }

  def sendSync(value: String) {
    val record = new ProducerRecord[String, String](configuration.Kafka.topics, value)
    try {
      producer.send(record).get()
    } catch {
      case e: Exception =>
        //todo: figure out what to do
        e.printStackTrace()
    }
  }

  def sendAsync(value: String) {
    val record = new ProducerRecord[String, String](configuration.Kafka.topics, value)
    val p = Promise[(RecordMetadata, Exception)]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) {
        p.success((metadata, exception))
      }
    })
  }

  def close(): Unit = producer.close()
}