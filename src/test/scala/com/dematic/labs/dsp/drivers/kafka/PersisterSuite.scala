package com.dematic.labs.dsp.drivers.kafka

import java.io.InputStream
import java.lang.Thread.sleep
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration
import com.dematic.labs.toolkit_bigdata.simulators.TestSignalProducer
import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper._
import org.junit.Rule
import org.junit.rules.TemporaryFolder
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

  @Rule var checkpoint = new TemporaryFolder

  val kafkaServer = new KafkaUnit
  val topicAndKeyspace = "persister"
  val numberOfSignalsPerSignalId = 100

  before {
    // create the checkpoint dir
    checkpoint.create()
    // 1) start kafka server and create topic, only one broker created during testing
    kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1")
    kafkaServer.startup()
    kafkaServer.createTopic(topicAndKeyspace, 1)
    // 2) start cassandra and create keyspace/table
    startEmbeddedCassandra(CASSANDRA_RNDPORT_YML_FILE)
    logger.info(s"kafka server = '${kafkaServer.getKafkaConnect}' cassandra = 'localhost:$getNativeTransportPort'")

    // 3) configure the driver
    val uri = getClass.getResource("/persister.conf").toURI
    val config = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile)
      .sparkCheckpointLocation(checkpoint.getRoot.getPath)
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .sparkCassandraConnectionHost("localhost")
      .sparkCassandraConnectionPort(getNativeTransportPort.toString)
      .build
    // set the configuration
    Persister.setDriverConfiguration(config)
  }

  after {
    kafkaServer.shutdown()
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
              if (!line.startsWith("//")) session.execute(line)
            }

            // 1) start the driver asynchronously
            Future {
              // start the driver
              Persister.main(Array[String]())
            }

            // 2) push signal to kafka
            new TestSignalProducer(kafkaServer.getKafkaConnect, topicAndKeyspace, numberOfSignalsPerSignalId,
              List(100, 200), "persisterProducer")

            // 3) query cassandra until all the signals have been saved
            val count: Future[Long] = Future {
              var numberOfSignals = 0L
              do {
                val row = session.execute("select count(*) from signals;").one()
                if (row != null) numberOfSignals = row.getLong("count")
                sleep(1000)
                // keep getting count until numberOfSignals have been persisted
              } while (numberOfSignals > 0)
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
