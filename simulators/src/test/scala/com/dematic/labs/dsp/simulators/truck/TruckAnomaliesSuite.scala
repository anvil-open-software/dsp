package com.dematic.labs.dsp.simulators.truck

import com.dematic.labs.dsp.simulators.TrucksFilter
import org.scalatest.FunSuite

class TruckAnomaliesSuite extends FunSuite {

  test("test simple anomaly filter") {
    val threshold = 10

    /* double point filtering with threshold*/
    val sendAnomalies = false
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold,  0, -40, 0))
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold,  22.0, 20, 30))
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold,  20.0, 10, 10))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  22.0, 20, 26))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  22.0, 20, 29))
  }

  test("test without anomaly filter") {
    val threshold = 0
    val sendAnomalies = true


    /* always send */
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  0, -40, 0))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  22.0, 100, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  10.0, 42, 10))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold,  22.0, 20, 66))

  }
}