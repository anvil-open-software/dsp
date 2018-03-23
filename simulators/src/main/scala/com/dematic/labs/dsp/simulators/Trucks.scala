package com.dematic.labs.dsp.simulators

import java.nio.charset.Charset
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MINUTES

import com.dematic.labs.dsp.simulators.configuration.TruckConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.google.common.util.concurrent.RateLimiter
import monix.eval.Task
import okhttp3.OkHttpClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.influxdb.dto.Query
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.control.NonFatal

/**
  *
  * To debug topic directly from kafka, use console consumer, i.e.:
  * $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server '10.207.222.11:9092,10.207.222.12:9092' --topic ccd_truck_temp
  */
object Trucks extends App {
  val logger: Logger = LoggerFactory.getLogger("Trucks")

  // load all the configuration
  private val config: TruckConfiguration = new TruckConfiguration.Builder().build

  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  val builder = new OkHttpClient.Builder().readTimeout(120, TimeUnit.SECONDS)
    .connectTimeout(120, TimeUnit.SECONDS)
  private val influxDB: InfluxDB = InfluxDBFactory.connect(config.getUrl,
    config.getUsername, config.getPassword, builder)
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

  var dateTimeFormatter = DateTimeFormatter.ISO_INSTANT
  // todo  parameterize
  val send_anomalies = false
  lazy val anomaly_threshhold = 7
  // can't use null in scala with Double

  import collection.JavaConversions._

  try {
    val queryStartTime = System.currentTimeMillis()
    val qr = influxDB.query(new Query(s"SELECT time, value FROM T_motTemp_Lft where time > " +
      s"'${config.getPredicateDateRangeLow}' AND time < '${config.getPredicateDateRangeHigh}' order by ASC",
      config.getDatabase))

    val queryExecutionTime = System.currentTimeMillis() - queryStartTime
    logger.info("influxdb query time=" + queryExecutionTime + " ms, returning rows=" + qr.getResults.size())

    qr.getResults foreach (it => {
      it.getSeries foreach (it => {
        timeSeries = timeSeries ++ it.getValues
      })
    })

    val lowTruckRange: Int = config.getTruckIdRangeLow
    val highTruckRange: Int = config.getTruckIdRangeHigh
    val numOfThreads = highTruckRange - lowTruckRange
    logger.info("Requested truck range " + lowTruckRange + " to " + highTruckRange)

    // define the global scheduler and the num and max of threads, will be based on the number of trucks, this could
    // cause thread starvation if too many trucks are defined, also, do not define less threads then the number of cores
    if (Runtime.getRuntime.availableProcessors < numOfThreads) {
      System.setProperty("scala.concurrent.context.maxThreads", numOfThreads.toString)
      System.setProperty("scala.concurrent.context.numThreads", numOfThreads.toString)
    }
    import monix.execution.Scheduler.Implicits.global

    // define how long to run the simulator
    val countdownTimer: CountdownTimer = new CountdownTimer
    countdownTimer.countDown(config.getDurationInMinutes.toInt)

    // create a list of task that will be dispatched on its own thread
    var tasks: List[Task[Unit]] = List()
    for (truckId <- lowTruckRange to highTruckRange) {
      tasks = tasks :+ Task {
        dispatchTruck(s"${config.getId}$truckId", countdownTimer)
      }
    }

    // execute the task asynchronously and keep a list of cancelable futures
    var futures: List[Future[Unit]] = List()
    tasks.foreach(task => {
      futures = futures :+ task.runAsync
    })
    // wait for all futures to complete, add 1 minute to make sure all msgs sent
    Await.result(Future.sequence(futures), Duration(config.getDurationInMinutes + 1, TimeUnit.MINUTES))
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

  // find point that is simple odd man out
  def shouldSend(prevData: Option[Double], curData: Double, nextData: Double): Boolean = {
    return (send_anomalies || prevData == None ||
      (Math.abs(curData - prevData.get) < anomaly_threshhold ||
        Math.abs(curData - nextData) < anomaly_threshhold))
  }

  def dispatchTruck(truckId: String, countdownTimer: CountdownTimer) {
    var index = randomIndex()
    var previousValue: Option[Double] = None

    // will limit sends to 1 per second
    val rateLimiter = RateLimiter.create(1, 0, MINUTES) // make configurable if needed
    // keep pushing msgs to kafka until timer finishes
    do {
      val data = (timeSeries get index).asInstanceOf[java.util.ArrayList[Any]]
      val nextData = (timeSeries get (index + 1)).asInstanceOf[java.util.ArrayList[Any]]
      // instead of preserving original time, use simulation time
      val messageTime = System.currentTimeMillis();
      // java.time.Instant = 2017-02-13T12:14:20.666Z
      val isoDateTime = dateTimeFormatter.format(Instant.ofEpochMilli(messageTime));
      val currentValue = data.get(1).asInstanceOf[Double]
      val nextValue = nextData.get(1).asInstanceOf[Double]
      // create the json
      val json =
      s"""{"truck":"$truckId","_timestamp":"$isoDateTime","channel":"T_motTemp_Lft","value":${data.get(1)}}"""
      // acquire permit to send, limits to 1 msg a second
      rateLimiter.acquire()
      // send to kafka
      if (shouldSend(previousValue, currentValue, nextValue)) {
        producer.send(new ProducerRecord[String, AnyRef](config.getTopics, json.getBytes(Charset.defaultCharset())))
      } else {
        logger.warn("Skipping point anomaly for " + truckId + ":" + previousValue + "," + currentValue  )
      }
      index = index + 1
      previousValue = Some(currentValue)
    } while (!countdownTimer.isFinished)
  }

  def randomIndex(): Int = {
    // generate a random index between 0 and half of the time series list
    Random.nextInt(timeSeries.size / 2)
  }
}
