package com.dematic.labs.dsp.data

/**
  * Supported Signal types.
  */
object SignalType extends Enumeration {
  type SignalType = Value
  val PICKER, SORTER, DMS = Value
}
