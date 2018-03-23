package com.dematic.labs.dsp.simulators.truck

import com.dematic.labs.dsp.simulators.Trucks
import org.scalatest.FunSuite

class TruckAnomaliesSuite extends FunSuite {

  test("test simple anomaly filter") {
    assert(true===Trucks.shouldSend(None,1,40))
    assert(true===Trucks.shouldSend(None,21,20))

    assert(false===Trucks.shouldSend(Some(0),-40,0))
    assert(true===Trucks.shouldSend(Some(22.0),20,40))
    assert(true===Trucks.shouldSend(Some(22.0),10,10))

  }
}