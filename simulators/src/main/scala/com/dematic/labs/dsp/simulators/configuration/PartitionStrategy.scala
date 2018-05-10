/**
  *  How a messages will be mapped to Kafka partition when partition count > 1
  *  1. random_per_request - good for maintaining load if use case permits
  *  3. default_keyed_partition - Kafka's consistent hashing algorithm to maintain ordering.
  *          However this can create hotpots
  *
  */
package com.dematic.labs.dsp.simulators.configuration

object PartitionStrategy extends Enumeration {
    type PartitionStrategy = Value
    val random_per_request, default_keyed_partition = Value
}
