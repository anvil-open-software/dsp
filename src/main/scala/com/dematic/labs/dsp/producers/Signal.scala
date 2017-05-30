package com.dematic.labs.dsp.producers

import scala.beans.BeanProperty

class Signal(@BeanProperty var id: String, @BeanProperty var value: String, @BeanProperty var timestamp: String) {
  override def toString: String = String format("%s, %s, %s", id, value, timestamp)
}
