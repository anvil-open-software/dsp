/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators.truck

import com.dematic.labs.dsp.simulators.{TruckSimulationState, TrucksFilter}
import org.scalatest.FunSuite

class TruckSignalFilterSuite extends FunSuite {

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

  test("test gap replay") {
    val tstate=new TruckSimulationState(100)
    val timenow = System.currentTimeMillis()
    // default is no gap replay
    assert(false === tstate.needsGapReplay(timenow))

    tstate.setGapSleepTime(timenow,100000)
    assert(true === tstate.needsGapReplay(timenow))
    tstate.setGapSleepTime(timenow,0)
    assert(false === tstate.needsGapReplay(timenow+1000))

    tstate.setGapSleepTime(timenow,5000)
    assert(true === tstate.needsGapReplay(timenow))
    assert(true === tstate.needsGapReplay(timenow+5000))
    assert(false === tstate.needsGapReplay(timenow+5001))
    assert(false === tstate.needsGapReplay(timenow+6000))
  }
}