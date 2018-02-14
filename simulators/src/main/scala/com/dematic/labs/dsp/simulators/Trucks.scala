package com.dematic.labs.dsp.simulators

import java.nio.charset.Charset
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MINUTES

import com.dematic.labs.dsp.simulators.configuration.TruckConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.google.common.util.concurrent.RateLimiter
import monix.eval.Task
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.influxdb.dto.Query
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.control.NonFatal

object Trucks extends App {
  val logger: Logger = LoggerFactory.getLogger("Trucks")

  // load all the configuration
  private val config: TruckConfiguration = new TruckConfiguration.Builder().build
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

  import collection.JavaConversions._

  try {
    val qr = influxDB.query(new Query(s"SELECT time, value FROM T_motTemp_Lft where time > " +
      s"'${config.getPredicateDateRangeLow}' AND time < '${config.getPredicateDateRangeHigh}' order by ASC",
      config.getDatabase))
    qr.getResults foreach (it => {
      it.getSeries foreach (it => {
        timeSeries = timeSeries ++ it.getValues
      })
    })

    val lowTruckRange: Int = config.getTruckIdRangeLow
    val highTruckRange: Int = config.getTruckIdRangeHigh
    val numOfThreads = highTruckRange - lowTruckRange
    // define the global scheduler and the num and max of threads, will be based on the number of trucks, this could
    // cause thread starvation if to many trucks are defined, also, do not define less threads then the number of cores
    if (Runtime.getRuntime.availableProcessors < numOfThreads) {
      System.setProperty("scala.concurrent.context.maxThreads", numOfThreads.toString)
      System.setProperty("scala.concurrent.context.numThreads", numOfThreads.toString)
    }
    import monix.execution.Scheduler.Implicits.global

    // define how long to run the simulator
    val countdownTimer: CountdownTimer = new CountdownTimer
    countdownTimer.countDown(config.getDurationInMinutes.toInt)

    // dispatch per truck to run on its own thread
    for (truckId <- lowTruckRange to highTruckRange) {
      Task {
        dispatchTruck(truckId.toString, countdownTimer)
      }.runAsync
    }
    // wait until config duration, do not want to close the shared kafka producer to soon
    Await.result(Future {
      while (!countdownTimer.isFinished) Thread.sleep(1000)
    }, Duration(config.getDurationInMinutes, MINUTES))
  } catch {
    case NonFatal(all) => logger.error("Error:", all)
  } finally {
    try {
      influxDB.close()
    } catch {
      case NonFatal(ex) => logger.error("Error: closing InfluxDb", ex)
    }
    try {
      producer.close(15, TimeUnit.SECONDS)
    } catch {
      case NonFatal(ex) => logger.error("Error: closing Kafka Producer", ex)
    }
  }

  def dispatchTruck(truckId: String, countdownTimer: CountdownTimer) {
    var index = randomIndex()
    // will limit sends to 1 per second
    val rateLimiter = RateLimiter.create(1, 0, MINUTES) // make configurable if needed
    // keep pushing msgs to kafka until timer finishes
    do {
      val data = (timeSeries get index).asInstanceOf[java.util.ArrayList[AnyRef]]
      // create the json
      val json =
        s"""{"truck":"$truckId","_timestamp":"${data.get(0)}","channel":"T_motTemp_Lft","value":${data.get(1)},"unit":"C${"\u00b0"}"}"""
      // acquire permit to send, limits to 1 msg a second
      rateLimiter.acquire()
      // send to kafka
      producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json.getBytes(Charset.defaultCharset())))
      index = index + 1
    } while (!countdownTimer.isFinished)
  }

  def randomIndex(): Int = {
    // generate a random index between 0 and half of the time series list
    Random.nextInt(timeSeries.size / 2)
  }
}
