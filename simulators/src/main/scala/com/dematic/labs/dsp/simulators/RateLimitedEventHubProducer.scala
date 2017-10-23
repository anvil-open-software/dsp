package com.dematic.labs.dsp.simulators

import java.nio.charset.Charset
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.data.Signal
import com.dematic.labs.dsp.data.SignalType.SORTER
import com.dematic.labs.dsp.data.Utils.toJson
import com.dematic.labs.dsp.simulators
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import com.google.common.util.concurrent.RateLimiter
import com.microsoft.azure.eventhubs.{ConnectionStringBuilder, EventData, EventHubClient}
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import scala.util.Random

object RateLimitedEventHubProducer extends App {
  // load all the configuration
  private val config = new simulators.EventHubConfiguration.Builder().build
  // define how long to run the throughput simulator
  private val countdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)
  // define the connection string
  val connStr =
    new ConnectionStringBuilder(config.getNamespace, config.getName, config.getAccessKeyName, config.getAccessKey)

  // generated values
  private val nextRandomValue = {
    val random = new Random
    () => {
      val num = random.nextInt()
      if (num < 0) num * -1 else num
    }
  }

  val lowSignalRange: Int = config.getSignalIdRangeLow
  val highSignalRange: Int = config.getSignalIdRangeHigh
  val producerId = config.getId

  for (signalId <- lowSignalRange to highSignalRange) {
    // on a separate thread
    Future {
      sendSignalsPerSignalId(signalId)
    }
  }

  def sendSignalsPerSignalId(signalId: Int) {
    // create one eventhub client per signal id, may need to use an event hub client pool
    val ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString)
    try {
      // will limit sends to 1 per second
      val rateLimiter = RateLimiter.create(1, 0, TimeUnit.MINUTES) // make configurable if needed
      while (!countdownTimer.isFinished) {
        rateLimiter.acquire()
        val bytes = toJson(new Signal(signalId, Instant.now.toString, SORTER.toString, nextRandomValue(),
          "%s-%s".format(producerId, signalId)))
          .getBytes(Charset.defaultCharset())
        ehClient.send(new EventData(bytes))
      }
    } finally {
      ehClient.close
    }
  }
}
