package com.dematic.labs.dsp.producers.kafka

import java.util.Properties

import com.dematic.labs.dsp.configuration.DriverConfiguration
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.Promise

case class Producer(configuration: DriverConfiguration) {
  val kafkaProps = new Properties()
  // required configuration
  kafkaProps.put(configuration.Kafka.BootstrapServersKey, configuration.Kafka.bootstrapServers)
  kafkaProps.put(configuration.Kafka.KeySerializerKey, configuration.Kafka.keySerializer)
  kafkaProps.put(configuration.Kafka.ValueSerializeKey, configuration.Kafka.valueSerializer)
  kafkaProps.put(configuration.Kafka.AcksKey, configuration.Kafka.acks)
  kafkaProps.put(configuration.Kafka.ProducerTypeKey, configuration.Kafka.producerType)
  kafkaProps.put(configuration.Kafka.RetriesKey, configuration.Kafka.retries)

  // connection to Kafka!
  private val producer = new KafkaProducer[String, String](kafkaProps)

  def send(value: String) {
    if (configuration.Kafka.producerType == "sync") sendSync(value) else sendAsync(value)
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