package com.dematic.labs.dsp.simulators

import java.time.Instant
import java.util
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.data.Signal
import com.dematic.labs.dsp.data.SignalType.SORTER
import com.dematic.labs.dsp.data.Utils._
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.dematic.labs.toolkit_bigdata.simulators.configuration.MinimalProducerConfiguration
import monix.eval.Task
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Will push random signal objects as JSON to the configured Kafka Broker for a fixed about of time.
  *
  */
object Throughput extends App {
  // load all the configuration
  private val config = new MinimalProducerConfiguration.Builder().build
  // define how long to run the throughput simulator
  private val countdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)

  // generated ids
  private val nextId = {
    var id: Long = 1
    () => {
      id += 1
      id
    }
  }

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
  properties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, Integer.toString(5 * 1000))
  properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE))
  properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Predef.long2Long(config.getBufferMemory))
  properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Predef.int2Integer(config.getBatchSize))
  properties.put(ProducerConfig.LINGER_MS_CONFIG, Predef.int2Integer(config.getLingerMs))
  properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType)

  private val producer = new KafkaProducer[String, AnyRef](properties)

  import monix.execution.Scheduler.Implicits.global
  // fire and forget, until timer is finished
  try {
    while (!countdownTimer.isFinished) {
      Task.now({
        val json = toJson(new Signal(nextId(), Instant.now.toString, SORTER.toString, nextRandomValue(), config.getId))
        producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json))
      }, global)
    }
  } finally {
    // close producer
    producer.close(15, TimeUnit.SECONDS)
  }
}