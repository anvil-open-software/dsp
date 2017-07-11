package com.dematic.labs.dsp.drivers.kafka

import java.io.InputStream
import java.lang.Thread.sleep
import java.util.concurrent.TimeUnit

import com.dematic.labs.toolkit_bigdata.simulators.diagnostics.Signals
import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

class PersisterSuite extends FunSuite with BeforeAndAfter {
  val logger: Logger = LoggerFactory.getLogger("PersisterSuite")

  val kafkaServer = new KafkaUnit
  val topicAndKeyspace = "persister"
  val expectedNumberOfSignals = 100

  before {
    // 1) start kafka server and create topic
    kafkaServer.startup()
    kafkaServer.createTopic(topicAndKeyspace)
    // 2) start cassandra and create keyspace/table
    startEmbeddedCassandra(CASSANDRA_RNDPORT_YML_FILE)
    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}' cassandra = 'localhost:$getNativeTransportPort'")

    // override properties from application.conf
    // driver properties
    System.setProperty("driver.appName", topicAndKeyspace)
    // spark properties
    System.setProperty("spark.cassandra.connection.host", "localhost")
    System.setProperty("spark.cassandra.connection.port", getNativeTransportPort.toString)
    System.setProperty("spark.cassandra.auth.username", "none")
    System.setProperty("spark.cassandra.auth.password", "none")
    // kafka properties, will be used for producer and driver
    System.setProperty("kafka.bootstrap.servers", kafkaServer.getKafkaConnect)
    System.setProperty("kafka.topics", topicAndKeyspace)
    // cassandra properties
    System.setProperty("cassandra.keyspace", topicAndKeyspace)
    // producer Id
    System.setProperty("producer.Id", "PersisterSuite")
  }

  after {
    // shutdown kafka
    try {
      kafkaServer.shutdown()
    } finally {
      // clear all properties
      System.clearProperty("driver.appName")
      System.clearProperty("spark.cassandra.connection.host")
      System.clearProperty("spark.cassandra.connection.port")
      System.clearProperty("spark.cassandra.auth.username")
      System.clearProperty("spark.cassandra.auth.password")
      System.clearProperty("kafka.bootstrap.servers")
      System.clearProperty("kafka.topics")
      System.clearProperty("cassandra.keyspace")
      System.clearProperty("producer.Id")
    }
  }

  test("complete DSP Persister test, push signals to kafka, spark consumes and persist to cassandra") {
    // create a cassandra cluster and connect and create keyspace and table
    // will close cluster
    using(getCluster) {
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

            // 1) start the driver asynchronously
            Future {
              // start the driver
              Persister.main(Array[String]())
            }

            // 2) push signal to kafka
            Signals.main(Array(expectedNumberOfSignals.toString))

            // 3) query cassandra until all the signals have been saved
            val count: Future[Long] = Future {
              var numberOfSignals = 0L
              do {
                val row = session.execute("select count(*) from signals;").one()
                if (row != null) numberOfSignals = row.getLong("count")
                sleep(1000)
                // keep getting count until numberOfSignals have been persisted
              } while (numberOfSignals != expectedNumberOfSignals)
              numberOfSignals
            }
            // succeeded
            count.onComplete({
              case Success(numberOfSignals) => logger.info(s"all signals '$numberOfSignals' found")
              case Failure(exception) => logger.error("unexpected error querying cassandra", exception)
            })
            // wait until we get a success, waiting 1 minute
            Await.ready(count, Duration.create(2, TimeUnit.MINUTES))
          }
        }
      }
    }
  }

  // Automatically close the resource
  def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
      logger.info(s"closed $resource")
    }
}