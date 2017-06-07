package com.dematic.labs.dsp.data

import scala.beans.BeanProperty
import scala.util.hashing.MurmurHash3

class Signal(@BeanProperty var id: Long, @BeanProperty var value: Int, @BeanProperty var timestamp: String,
             @BeanProperty val generatorId: String) {
  override def equals(other: Any): Boolean = other match {
    case that: Signal => (that canEqual this) &&
      that.id == this.id &&
      that.value == this.value &&
      that.timestamp == this.timestamp
      that.generatorId == this.generatorId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Signal]

  override def hashCode(): Int = MurmurHash3.seqHash(List(id, value, timestamp, generatorId))

  override def toString = s"Signal($id, $value, $timestamp, $generatorId)"
}
