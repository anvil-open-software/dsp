package com.dematic.labs.dsp.simulators

import java.time.Instant

import com.dematic.labs.dsp.configuration.DriverConfiguration
import com.dematic.labs.dsp.data.Utils.toJson
import com.dematic.labs.dsp.data.{CountdownTimer, Signal}
import com.dematic.labs.dsp.producers.kafka.Producer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object Throughput extends App {
  // define how long to run the throughput simulator
  CountdownTimer.countDown(args(0).toInt)
  // configuration
  val config = new DriverConfiguration
  // overridden value specific to producer
  val generatorId = s"${config.Driver.appName}${args(1)}"

  val producer = Producer(config)
  // generated values
  private val nextId = {
    var id: Long = 1
    () => {
      id += 1
      id
    }
  }
  private val nextRandomValue = {
    val random = new Random
    () => {
      random.nextInt()
    }
  }

  // fire and forget, until timer is finished
  while (!CountdownTimer.isFinished) {
    val result = Future {
      // create random json
      val json = toJson(new Signal(nextId(), nextRandomValue(), Instant.now.toString, generatorId))
      producer.send(json)
    }
    // only print exception if, something goes wrong
    result onFailure {
      case t => println(s"Unexpected Error: ${t.printStackTrace()}")
    }
  }
  producer.close()
}
