/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

/**
  *  How a messages will be mapped to Kafka partition when partition count > 1
  *  1. random_per_request - good for maintaining load if use case permits
  *  3. default_keyed_partition - Kafka's consistent hashing algorithm to maintain ordering.
  *          However this can create hotpots
  *  2.  SAME_PARTITION_PER_THREAD - everything on thread goes to same partition, uses modulo based on thread id
  *
  *
  */
package com.dematic.labs.dsp.simulators.configuration

object PartitionStrategy extends Enumeration {
    type PartitionStrategy = Value
    val RANDOM_PER_REQUEST,
        DEFAULT_KEYED_PARTITION,
        SAME_PARTITION_PER_THREAD,
        SAME_PARTITION_PER_THREAD_RANDOM_ID = Value
}
