/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.dsp.data

/**
  * Supported Signal types.
  */
object SignalType extends Enumeration {
  type SignalType = Value
  val PICKER, SORTER, DMS = Value
}
