package com.dematic.labs.dsp.data

import java.time.Instant

import com.dematic.labs.dsp.data.SignalType.SORTER
import org.scalatest.FunSuite

class DataSuite extends FunSuite {
  test("signal to json, json to signal") {
    val signal = new Signal(123, Instant.now().toString, SORTER.toString, 5555, "DataSuite")
    val toJson = Utils.toJson(signal)
    val fromJson = Utils.fromJson[Signal](toJson)
    assert(signal === fromJson)
  }
}
