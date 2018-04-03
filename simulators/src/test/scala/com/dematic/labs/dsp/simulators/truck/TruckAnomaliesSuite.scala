package com.dematic.labs.dsp.simulators.truck

import com.dematic.labs.dsp.simulators.TrucksFilter
import org.scalatest.FunSuite

class TruckAnomaliesSuite extends FunSuite {

  test("test simple anomaly filter") {
    val threshold = 10
    assert(true === TrucksFilter.shouldSend(sendAnomalies = true, threshold, None, 1, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = false, threshold, None, 1, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = true, threshold, None, 21, 20))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = false, threshold, None, 21, 20))

    /* double point filtering with threshold*/
    val sendAnomalies = false
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(0), -40, 0))
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 20, 30))
    assert(false === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(20.0), 10, 10))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 20, 26))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 20, 29))
  }

  test("test without anomaly filter") {
    val threshold = 0
    val sendAnomalies = true
    assert(true === TrucksFilter.shouldSend(sendAnomalies = true, threshold, None, 1, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = false, threshold, None, 1, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = true, threshold, None, 21, 20))
    assert(true === TrucksFilter.shouldSend(sendAnomalies = false, threshold, None, 21, 20))

    /* always send */
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(0), -40, 0))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 20, 40))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 10, 10))
    assert(true === TrucksFilter.shouldSend(sendAnomalies, threshold, Some(22.0), 20, 26))

  }
}