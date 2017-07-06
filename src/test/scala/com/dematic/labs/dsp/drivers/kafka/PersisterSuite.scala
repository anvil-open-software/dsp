package com.dematic.labs.dsp.drivers.kafka


import com.datastax.driver.core.Cluster
import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfter, FunSuite}

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
    val cluster: Cluster =
      Cluster.builder().addContactPoint(EmbeddedCassandraServerHelper.getHost).withPort(EmbeddedCassandraServerHelper.getNativeTransportPort).build

    try {
      val session = cluster.connect
      session.execute(s"CREATE KEYSPACE if not exists $topicAndKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
      // ensure keyspace was created
      assert(cluster.getMetadata.getKeyspace(topicAndKeyspace).getName === topicAndKeyspace)
      // create cassandra table


    } finally {
      cluster.close()
    }
  }
}
