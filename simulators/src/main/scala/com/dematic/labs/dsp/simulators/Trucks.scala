package com.dematic.labs.dsp.simulators

import java.nio.charset.Charset
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MINUTES

import com.dematic.labs.dsp.simulators.configuration.PartitionStrategy.PartitionStrategy
import com.dematic.labs.dsp.simulators.configuration.{PartitionStrategy, TruckConfiguration}
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
  * This simulator is meant to be run in parallel along side other jvms on the same and different instances
  *
  * To debug topic directly from kafka, use console consumer, i.e.:
  * $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server '10.207.222.11:9092,10.207.222.12:9092' --topic icd_truck_temp
  *
  */

object Trucks extends App {
  val logger: Logger = LoggerFactory.getLogger("Trucks")

  // load all the configuration
  private val config: TruckConfiguration = new TruckConfiguration.Builder().build

  // create the connection to influxDb with more generous timeout instead of default 10 seconds
  val builder = new OkHttpClient.Builder().readTimeout(180, TimeUnit.SECONDS)
    .connectTimeout(120, TimeUnit.SECONDS)
    .writeTimeout(180, TimeUnit.SECONDS)
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
  var formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.'SSSX")

  val sendAnomalies = config.getAnomaliesSend
  val anomalyThreshhold = config.getAnomaliesFilterThreshhold
  val rateLimiterPermitsPerSecond = config.getRateLimiterPermitsPerSecond
  val gapThresholdMilliseconds = config.getGapThresholdInMillis

  import collection.JavaConversions._

  try {
    val queryStartTime = System.currentTimeMillis()

    val qr = influxDB.query(new Query(s"SELECT time, value FROM T_motTemp_Lft where time > " +
      s"'${config.getPredicateDateRangeLow}' AND time < '${config.getPredicateDateRangeHigh}' order by ASC",
      config.getDatabase),TimeUnit.MILLISECONDS)

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
    val trucksPerThread = config.getTrucksPerThread
    logger.info("Requested truck range " + lowTruckRange + " to " + highTruckRange
      + " with availableProcessors=" + Runtime.getRuntime.availableProcessors)


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

    val partitionCount = producer.partitionsFor(config.getTopics).size()
    val permitsPerSecond = config.getRateLimiterPermitsPerSecond
    // create a list of task that will be dispatched on its own thread
    var tasks: List[Task[Unit]] = List()
    for (truckIdx <- lowTruckRange to highTruckRange) {
      tasks = tasks :+ Task {
        // try
        if (trucksPerThread == 1) {
          dispatchTruck(s"${config.getId}$truckIdx", countdownTimer)
        } else {
          // randomly generate the partition for that thread
          val partitionId =assignPartitionId(config.getPartitionStrategy,partitionCount,truckIdx)
          dispatchTrucks(partitionId, s"${config.getId}$truckIdx", trucksPerThread, countdownTimer,permitsPerSecond)
        }
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


  def dispatchTruck(truckId: String, countdownTimer: CountdownTimer) {
    var index = randomIndex()
    // will limit sends to 1 per second
    val rateLimiter = RateLimiter.create(1, 0, MINUTES) // make configurable if needed
    var previousValue: Double = 0.0
    // keep pushing msgs to kafka until timer finishes
    do {
      val data = (timeSeries get index).asInstanceOf[java.util.ArrayList[Any]]
      val prevData = (timeSeries get (index - 1)).asInstanceOf[java.util.ArrayList[Any]]
      val nextData = (timeSeries get (index + 1)).asInstanceOf[java.util.ArrayList[Any]]

      // instead of preserving original time, use simulation time
      val messageTime = System.currentTimeMillis()
      // sometimes time unit is "2017-02-03T11:55:50Z" and othertimes nano...
      // java.time.Instant = 2017-02-13T12:14:20.666Z
      val isoDateTime = dateTimeFormatter.format(Instant.ofEpochMilli(messageTime))
      val currentValue = data.get(1).asInstanceOf[Double]
      val nextValue = nextData.get(1).asInstanceOf[Double]
      previousValue = prevData.get(1).asInstanceOf[Double]

      if (gapThresholdMilliseconds > 0) {
        // sleep if actual time between points is more than gapThresholdMilliseconds
        try {

          val prevHistoryTime = prevData.get(0).asInstanceOf[Double].toLong
          val currentHistoryTime = data.get(0).asInstanceOf[Double].toLong

          val historicalGapMs = currentHistoryTime - prevHistoryTime
          if (historicalGapMs > gapThresholdMilliseconds) {
            logger.warn("Sleeping during gap for " + truckId + " for " + historicalGapMs + " ms")
            Thread.sleep(historicalGapMs)
          }
        } catch {
          case NonFatal(ex) => logger.error("InfluxDB time not parseable value=" + currentValue, ex)
        }
      }

      // create the json
      val json =
      s"""{"truck":"$truckId","_timestamp":"$isoDateTime","channel":"T_motTemp_Lft","value":${data.get(1)}}"""
      // acquire permit to send, limits to 1 msg a second
      rateLimiter.acquire()
      // send to kafka

      if (TrucksFilter.shouldSend(sendAnomalies, anomalyThreshhold, previousValue, currentValue, nextValue)) {

        config.getPartitionStrategy match {
          case PartitionStrategy.DEFAULT_KEYED_PARTITION => producer.send(new ProducerRecord[String, AnyRef](config.getTopics, truckId, json.getBytes(Charset.defaultCharset())))

          // random per transaction which can cause shuffles if order is needed
          case _ => producer.send(new ProducerRecord[String, AnyRef](config.getTopics, truckId, json.getBytes(Charset.defaultCharset())))
        }

      } else {
        logger.warn("Skipping point anomaly for " + truckId + ":" + previousValue + "," + currentValue)
      }
      index = index + 1
      previousValue = currentValue
    } while (!countdownTimer.isFinished)
  }

  /**
    * Single thread task that will launch multiple trucks based on trucksPerThread
    *
    * @param baseTruckId
    * @param trucksPerThread
    * @param countdownTimer
    */
  def dispatchTrucks(partitionIdx: Int,
                     baseTruckId: String,
                     trucksPerThread: Int,
                     countdownTimer: CountdownTimer,
                     permitsPerSecond: Double ) {

    var index = new Array[Int](trucksPerThread)
    var truckStatesForThread = new Array[TruckSimulationState](trucksPerThread)
    for (i <- 0 to trucksPerThread - 1) {
      index(i) = randomIndex()
      truckStatesForThread(i)=new TruckSimulationState(i)
    }

    // will limit sends to 1 per second
    val rateLimiter = RateLimiter.create(permitsPerSecond, 0, MINUTES) // make configurable if needed
    // keep pushing msgs to kafka until timer finishes
    do {
      // shove into TruckSimulationState if time
      var data = new Array[java.util.ArrayList[Any]](trucksPerThread)
      var prevData = new Array[java.util.ArrayList[Any]](trucksPerThread)
      var nextData = new Array[java.util.ArrayList[Any]](trucksPerThread)
      var currentValue = new Array[Double](trucksPerThread)
      var nextValue = new Array[Double](trucksPerThread)
      var previousValue = new Array[Double](trucksPerThread)

      for (i <- 0 to trucksPerThread - 1) {

        data(i) = (timeSeries get index(i)).asInstanceOf[java.util.ArrayList[Any]]
        prevData(i) = (timeSeries get (index(i) - 1)).asInstanceOf[java.util.ArrayList[Any]]
        nextData(i) = (timeSeries get (index(i) + 1)).asInstanceOf[java.util.ArrayList[Any]]

        previousValue(i) = prevData(i).get(1).asInstanceOf[Double]
        currentValue(i) = data(i).get(1).asInstanceOf[Double]
        nextValue(i) = nextData(i).get(1).asInstanceOf[Double]

        if (gapThresholdMilliseconds > 0) {
          // sleep if actual time between points is more than gapThresholdMilliseconds
          try {

            val prevHistoryTime = prevData(i).get(0).asInstanceOf[Double].toLong
            val currentHistoryTime = data(i).get(0).asInstanceOf[Double].toLong

            val historicalGapMs = currentHistoryTime - prevHistoryTime
            if (historicalGapMs > gapThresholdMilliseconds) {
              truckStatesForThread(i).setGapSleepTime( System.currentTimeMillis(), historicalGapMs)
              logger.warn("Sleep gap" + baseTruckId + i.toString + " for "  + historicalGapMs + " ms with prevtime="
                + dateTimeFormatter.format(Instant.ofEpochMilli(prevHistoryTime))
                 + " and current=" +dateTimeFormatter.format(Instant.ofEpochMilli(currentHistoryTime)))
            }
          } catch {
            case NonFatal(ex) => logger.error("Junk data- InfluxDB time not parseable value=" + currentValue(i), ex)
          }
        }

      }
      rateLimiter.acquire()
      for (i <- 0 to trucksPerThread - 1) {
        val truckId = baseTruckId + i.toString
        // instead of preserving original time, use simulation time
        val messageTime = System.currentTimeMillis()
        val passesAnomalyFilter=TrucksFilter.shouldSend(sendAnomalies, anomalyThreshhold, previousValue(i), currentValue(i), nextValue(i))
        // send to kafka
        if (!(truckStatesForThread(i).needsGapReplay(messageTime)) && passesAnomalyFilter) {
          // create the json

          // java.time.Instant = 2017-02-13T12:14:20.666Z
          val isoDateTime = dateTimeFormatter.format(Instant.ofEpochMilli(messageTime))
          val json =
            s"""{"truck":"$truckId","_timestamp":"$isoDateTime","channel":"T_motTemp_Lft","value":${data(i).get(1)}}"""
          // acquire permit to send, limits to 1 msg a second

          config.getPartitionStrategy match {
            case PartitionStrategy.DEFAULT_KEYED_PARTITION => producer.send(new ProducerRecord[String, AnyRef](
              config.getTopics, truckId, json.getBytes(Charset.defaultCharset())))

            case PartitionStrategy.SAME_PARTITION_PER_THREAD => producer.send(new ProducerRecord[String, AnyRef](
              config.getTopics, partitionIdx, truckId, json.getBytes(Charset.defaultCharset())))

            // random per transaction which can cause shuffles if order is needed
            case _ => producer.send(new ProducerRecord[String, AnyRef](
              config.getTopics, truckId, json.getBytes(Charset.defaultCharset())))
          }

        } else {
           if (!passesAnomalyFilter)
             logger.warn("Skipping point anomaly for " + truckId + ":" + previousValue(i) + "," + currentValue(i))
        }
        index(i) = index(i) + 1
        previousValue(i) = currentValue(i)
      }
    } while (!countdownTimer.isFinished)
  }

  def randomIndex(): Int = {
    // generate a random index between 1 and half of the time series list
    Random.nextInt(timeSeries.size / 2) + 2
  }
  def assignPartitionId(partitionStrategy: PartitionStrategy, partitionCount:Int, truckIdx:Int) : Int = {
    partitionStrategy match {
      case PartitionStrategy.SAME_PARTITION_PER_THREAD =>  Math.floorMod(truckIdx, partitionCount)
      // random terribly uneven
      case _ => Random.nextInt(partitionCount)
    }

  }
}

/**
  * This object to be tucked out separately so component tests can run,
  * otherwise Trucks forces a Kafka server to be up on instantiation
  */
object TrucksFilter {
  /**
    *
    * @return false if that differs by anomaly_threshold in either direction
    */
  def shouldSend(sendAnomalies: Boolean,
                 anomalyThreshhold: Int,
                 prevData: Double,
                 curData: Double,
                 nextData: Double): Boolean = {
    sendAnomalies || (Math.abs(curData - prevData) < anomalyThreshhold &&
      Math.abs(curData - nextData) < anomalyThreshhold)
  }

}
