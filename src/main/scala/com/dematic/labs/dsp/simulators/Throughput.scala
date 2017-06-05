package com.dematic.labs.dsp.simulators

import java.time.Instant

import com.dematic.labs.dsp.configuration.DriverConfiguration
import com.dematic.labs.dsp.data.Utils.toJson
import com.dematic.labs.dsp.data.{CountdownTimer, Signal}
import com.dematic.labs.dsp.producers.kafka.Producer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Throughput extends App {
  val config = new DriverConfiguration
  val producer = Producer(config)
  CountdownTimer.countDown(1)
  // fire and forget, until timer is finished
  while (!CountdownTimer.isFinished) {
    val result = Future {
      // create random json
      val json = toJson(new Signal(generateRandomId(), generateRandomValue(), Instant.now.toString))
      producer.send(json)
    }
    // only print exception if, something goes wrong
    result onFailure {
      case t => println(s"Unexpected Error: ${t.getMessage}")
    }
  }
  producer.close()

  private def generateRandomId(): String = {
    ""
  }

  private def generateRandomValue(): String = {
    ""
  }
}
