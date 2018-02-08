package com.dematic.labs.dsp.simulators

import com.dematic.labs.dsp.simulators.configuration.TruckConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.CountdownTimer
import monix.eval.Task
import org.influxdb.InfluxDBFactory

object Trucks extends App {
  // load all the configuration
  private val config = new TruckConfiguration.Builder().build
  // define how long to run the simulator
  private val countdownTimer = new CountdownTimer
  countdownTimer.countDown(config.getDurationInMinutes.toInt)
  // create the connection to influxDb
  val influxDB = InfluxDBFactory.connect(config.getUrl, config.getUsername, config.getPassword)

  import monix.execution.Scheduler.Implicits.global

  try {
    val lowTruckRange: Int = config.getTruckIdRangeLow
    val highTruckRange: Int = config.getTruckIdRangeHigh
    while (!countdownTimer.isFinished) {
      // finish the cycle then check if timer is finished
      for (truckId <- lowTruckRange to highTruckRange) {
        Task.now({}, global)
      }
    }
  } finally {
    influxDB.close()
  }

}
