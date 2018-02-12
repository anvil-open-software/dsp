package com.dematic.labs.dsp.simulators

import java.util
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.simulators.configuration.TruckConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import monix.eval.Task
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Query
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

object Trucks extends App {
  val logger: Logger = LoggerFactory.getLogger("Trucks")

  // load all the configuration
  private val config = new TruckConfiguration.Builder().build
  // create the connection to influxDb
  private val influxDB = InfluxDBFactory.connect(config.getUrl, config.getUsername, config.getPassword)
  // shared kafka producer, used for batching and compression
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

  import collection.JavaConversions._


  try {
    val timeSeries = List()

    // query influxDb to get the next set of data
    //todo: figure out how to get the next range of data
    val qr = influxDB.query(new Query("SELECT time, value FROM T_motTemp_Lft limit 10000", config.getDatabase))
    qr.getResults foreach (it => {
      it.getSeries foreach (it => {
        timeSeries.add(it.getValues)
      })
    })

    val lowTruckRange: Int = config.getTruckIdRangeLow
    val highTruckRange: Int = config.getTruckIdRangeHigh

    // dispatch per truck to run on its own thread
    for (truckId <- lowTruckRange to highTruckRange) {
      Task {
        dispatchTruck(truckId.toString)
      }.runAsync
    }
  } finally {
    try {
      influxDB.close()
    } catch {
      case NonFatal(t) => logger.error("Error: closing InfluxDb", t)
    }
    try {
      producer.close(15, TimeUnit.SECONDS)
    } catch {
      case NonFatal(t) => logger.error("Error: closing Kafka Producer", t)
    }
  }

  // define how long to run the simulator
  private val countdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)

  def dispatchTruck(truckId: String) {
    // keep pushing msgs to kafka until timer finishes
    while (!countdownTimer.isFinished) {


      // send to kafka
      producer.send(new ProducerRecord[String, AnyRef](config.getTopics, ""))
    }
  }
}
