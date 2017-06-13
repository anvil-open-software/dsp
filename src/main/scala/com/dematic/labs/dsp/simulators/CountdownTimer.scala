package com.dematic.labs.dsp.simulators

//todo: remove, put in commons

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Timer, TimerTask}

object CountdownTimer {
  private var finished = false

  def countDown(inMinutes: Int): Unit = {
    val nowPlusMinutes = Instant.now.plus(inMinutes, ChronoUnit.MINUTES)
    val timer = new Timer
    timer.scheduleAtFixedRate(new TimerTask() {
      override def run(): Unit = {
        if (Instant.now.isAfter(nowPlusMinutes)) try
          timer.cancel()
        finally finished = true
      }
    }, 0, 60 * 1000L)
  }

  def isFinished: Boolean = finished
}
