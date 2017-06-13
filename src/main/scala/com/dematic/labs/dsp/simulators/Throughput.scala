package com.dematic.labs.dsp.simulators

import java.time.Instant
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.dematic.labs.dsp.configuration.DriverConfiguration._
import com.dematic.labs.dsp.data.Signal
import com.dematic.labs.dsp.data.Utils.toJson
import com.dematic.labs.dsp.producers.kafka.Producer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Will push random signal objects as JSON to the configured Kafka Broker for a fixed about of time. Application
  * takes 2 parameters: duration timeInMinutes, unique producer name.
  *
  * For example: 3 producer_1 // application will execute for 3 minutes and its unique name is producer_1
  */
object Throughput extends App {
  // define how long to run the throughput simulator
  CountdownTimer.countDown(args(0).toInt)
  // overridden value specific to producer
  val generatorId = s"${Driver.appName}${args(1)}"
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
      random.nextInt()
    }
  }

  // number of threads on the box
  val numWorkers = sys.runtime.availableProcessors
  // underlying thread pool with a fixed number of worker threads, backed by an unbounded LinkedBlockingQueue[Runnable]
  // define a DiscardPolicy to silently passes over RejectedExecutionException
  val executorService = new ThreadPoolExecutor(numWorkers, numWorkers, 0L, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable], Executors.defaultThreadFactory, new DiscardPolicy)

  // the ExecutionContext that wraps the thread pool
  implicit val ec = ExecutionContext.fromExecutorService(executorService)

  // fire and forget, until timer is finished
  val producer = new Producer
  try {
    while (!CountdownTimer.isFinished) {
      val result = Future {
        // create random json
        val json = toJson(new Signal(nextId(), Instant.now.toString, nextRandomValue(), generatorId))
        producer.send(json)
      }
      // only print exception if, something goes wrong
      result onFailure {
        case t => println(s"Unexpected Error:\n ${t.printStackTrace()}")
      }
    }
  } finally {
    // close execution context
    ec.shutdownNow()
    // close producer
    producer.close()
  }
}
