/*
 *
 * This file contains all Cassandra table creation CQL for the com.dematic.labs.dsp.drivers.kafka.Persister driver
 *
 * You must first create your cassandra keyspace with statement like:
 *
 * CREATE KEYSPACE if not exists your_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
 *
 * Run this file with cqlsh with -k option to include keyspace
 *
 */



// create signals table

CREATE TABLE if not exists signals (
 id bigint, timestamp timestamp, value int, producerId text, PRIMARY KEY(id, timestamp)
) WITH CLUSTERING ORDER BY(timestamp desc);