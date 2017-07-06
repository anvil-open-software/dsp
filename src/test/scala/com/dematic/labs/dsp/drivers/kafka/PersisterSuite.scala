package com.dematic.labs.dsp.drivers.kafka

import java.io.InputStream

import com.datastax.driver.core.Cluster
import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.io.Source

class PersisterSuite extends FunSuite with BeforeAndAfter {

  val kafkaServer = new KafkaUnit
  val topicAndKeyspace = "persister"

  before {
    // 1) start kafka server and create topic
    kafkaServer.startup()
    kafkaServer.createTopic(topicAndKeyspace)
    // 2) start cassandra and create keyspace/table
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
  }

  after {
    // shutdown kafka
    kafkaServer.shutdown()
  }

  test("complete DSP Persister test, push signals to kafka, spark consumes and persist to cassandra") {
    // create a cassandra cluster and connect and create keyspace and table
    using(Cluster.builder().
      addContactPoint(EmbeddedCassandraServerHelper.getHost).
      withPort(EmbeddedCassandraServerHelper.getNativeTransportPort).build) {
      cluster => {
        using(cluster.connect) {
          session => {
            session.execute(s"CREATE KEYSPACE if not exists $topicAndKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
            // ensure keyspace was created
            assert(cluster.getMetadata.getKeyspace(topicAndKeyspace).getName === topicAndKeyspace)
            // move to keyspace
            session.execute(s"USE $topicAndKeyspace;")
            // create table from cql
            val stream: InputStream = getClass.getResourceAsStream("/persister.cql")
            for (line <- Source.fromInputStream(stream).getLines) {
              if (!line.startsWith("#")) session.execute(line)
            }
          }
        }
      }
    }
  }

  // Automatically closing the resource
  def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
