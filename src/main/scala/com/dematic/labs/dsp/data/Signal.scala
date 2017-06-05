package com.dematic.labs.dsp.data

import scala.beans.BeanProperty
import scala.util.hashing.MurmurHash3

class Signal(@BeanProperty var id: String, @BeanProperty var value: String, @BeanProperty var timestamp: String) {
  override def equals(other: Any): Boolean = other match {
    case that: Signal => (that canEqual this) &&
      that.id == this.id &&
      that.value == this.value &&
      that.timestamp == this.timestamp
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Signal]

  override def hashCode(): Int = MurmurHash3.seqHash(List(id, value, timestamp))

  override def toString: String = String format("%s, %s, %s", id, value, timestamp)
}
