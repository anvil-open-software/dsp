/**
  *  How a messages will be mapped to Kafka partition when partition count > 1
  *  random_per_request - good for maintaining load if use case permits
  *  default_keyed_partition - Kafka's consistent hashing algorithm to maintain ordering
  *
  */
package com.dematic.labs.dsp.simulators.configuration

object PartitionStrategy extends Enumeration {
    type PartitionStrategy = Value
    val random_per_request, default_keyed_partition = Value
}
