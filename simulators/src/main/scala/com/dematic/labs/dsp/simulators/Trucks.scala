package com.dematic.labs.dsp.simulators

import java.util
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.simulators.configuration.TruckConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import monix.eval.Task
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.influxdb.dto.Query
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random
import scala.util.control.NonFatal

object Trucks extends App {
  val logger: Logger = LoggerFactory.getLogger("Trucks")

  // load all the configuration
  private val config: TruckConfiguration = new TruckConfiguration.Builder().build

  // define how long to run the simulator
  private val countdownTimer: CountdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)

  // create the connection to influxDb
  private val influxDB: InfluxDB = InfluxDBFactory.connect(config.getUrl, config.getUsername, config.getPassword)
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

  private val producer: KafkaProducer[String, AnyRef] = new KafkaProducer[String, AnyRef](properties)
  private var timeSeries: List[AnyRef] = List()

  import monix.execution.Scheduler.Implicits.global

  import collection.JavaConversions._

  try {
    val qr =
      influxDB.query(new Query("SELECT time, value FROM T_motTemp_Lft where time > '2017-01-01' AND time < '2017-03-01' order by DESC", config.getDatabase))
    qr.getResults foreach (it => {
      it.getSeries foreach (it => {
        timeSeries = timeSeries ++ it.getValues
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

  def dispatchTruck(truckId: String) {
    var index = randomIndex()
    // keep pushing msgs to kafka until timer finishes
    while (!countdownTimer.isFinished) {
      val data = (timeSeries get index).asInstanceOf[java.util.ArrayList[AnyRef]]
      // create the json
      val json =
        s"""{"truck":"$truckId","_timestamp":"${data.get(0)}","channel":"T_motTemp_Lft","value":${data.get(1)},"unit":"C${"\u00b0"}"}"""
      // send to kafka
      producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json))
      index = index + 1
    }
  }

  def randomIndex(): Int = {
    // generate a random index between 0 and half of the time series list
    Random.nextInt(timeSeries.size / 2)
  }
}
