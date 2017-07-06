package com.dematic.labs.dsp.drivers.kafka

import java.io.InputStream

import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.language.reflectiveCalls

class PersisterSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("PersisterSuite")

  val kafkaServer = new KafkaUnit
  val topicAndKeyspace = "persister"

  before {
    // 1) start kafka server and create topic
    kafkaServer.startup()
    kafkaServer.createTopic(topicAndKeyspace)
    // 2) start cassandra and create keyspace/table
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}' cassandra = 'localhost:${EmbeddedCassandraServerHelper.getNativeTransportPort}'")
  }

  after {
    // shutdown kafka
    kafkaServer.shutdown()
  }

  test("complete DSP Persister test, push signals to kafka, spark consumes and persist to cassandra") {
    // create a cassandra cluster and connect and create keyspace and table
    println(kafkaServer.getBrokerPort)
    // will close cluster
    using(EmbeddedCassandraServerHelper.getCluster) {
      cluster => {
        // will close session
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
            // override properties from application.conf
            // driver properties
            System.setProperty("driver.appName", topicAndKeyspace)
            // spark properties
            System.setProperty("spark.cassandra.connection.host", "localhost")
            System.setProperty("spark.cassandra.connection.port", EmbeddedCassandraServerHelper.getNativeTransportPort.toString)
            System.setProperty("spark.cassandra.auth.username", "none")
            System.setProperty("spark.cassandra.auth.password", "none")
            // kafka properties
            System.setProperty("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
            System.setProperty("kafka.topics", topicAndKeyspace)
            // cassandra properties
            System.setProperty("cassandra.keyspace", topicAndKeyspace)
            // start and deploy spark driver
            Persister.main(Array[String]())
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
