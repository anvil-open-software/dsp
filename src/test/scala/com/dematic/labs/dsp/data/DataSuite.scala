package com.dematic.labs.dsp.data

import java.sql.Timestamp
import java.time.Instant

import org.scalatest.FunSuite

class DataSuite extends FunSuite {
  test("signal to json, json to signal") {
    val signal = new Signal("123", "5555", Instant.now().toString)
    val toJson = Utils.toJson(signal)
    val fromJson = Utils.fromJson[Signal](toJson)
    assert(signal === fromJson)
  }
}
