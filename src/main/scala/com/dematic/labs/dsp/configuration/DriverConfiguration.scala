package com.dematic.labs.dsp.configuration

import com.typesafe.config._

class DriverConfiguration() {
  private val config =  ConfigFactory.load()
  private lazy val root = config.getConfig("driverApp")
}
