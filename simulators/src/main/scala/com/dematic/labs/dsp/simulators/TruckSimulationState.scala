/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.simulators

/**
  * Stores gap replay state for single truck
  */
class TruckSimulationState(val index: Long) {
  // val prevHistoryTime
  private val initialIndex = index
  // holds the last known gap replay state
  private var sleepUntil: Long = 0

  def needsGapReplay(timeNow: Long): Boolean = {
      timeNow <= sleepUntil
  }

  def setGapSleepTime(startTime: Long, historicalGapMs: Long) {
    sleepUntil = startTime + historicalGapMs
  }
}
