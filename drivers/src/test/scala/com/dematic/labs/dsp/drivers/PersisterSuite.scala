package com.dematic.labs.dsp.drivers

import java.io.InputStream
import java.lang.Thread.sleep
import java.nio.file.Paths
import java.util.concurrent.{Executors, TimeUnit}

import com.dematic.labs.dsp.drivers.configuration.DriverUnitTestConfiguration
import com.dematic.labs.dsp.data.SignalType.SORTER
import com.dematic.labs.dsp.simulators.TestSignalProducer
import info.batey.kafka.unit.KafkaUnit
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
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

  private val kafkaServer = new KafkaUnit
  private val topicAndKeyspace = "persister"
  private val numberOfSignalsPerSignalId = 10
  private val expectedNumberOfSignals = 110 // numberOfSignalsPerId * numberOfIds = 11 * 10
  private val es = Executors.newCachedThreadPool

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
      .sparkCheckpointLocation(checkpoint.getRoot.getPath + "/persister")
      .kafkaBootstrapServer(kafkaServer.getKafkaConnect)
      .sparkCassandraConnectionHost("localhost")
      .sparkCassandraConnectionPort(getNativeTransportPort.toString)
      .build
    // set the configuration
    Persister.setDriverConfiguration(config)
  }

  after {
    try {
      kafkaServer.shutdown()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try {
      cleanEmbeddedCassandra()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
    try {
      es.shutdownNow()
    } catch {
      case _: Throwable => // Catching all exceptions and not doing anything with them
    }
  }

  test("complete DSP Persister test, push signals to kafka, spark consumes and persist to cassandra") {
    val session = EmbeddedCassandraServerHelper.getSession

    // 1) create a cassandra cluster and connect and create keyspace and table
    {
      session.execute(s"CREATE KEYSPACE if not exists $topicAndKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
      // ensure keyspace was created
      assert(EmbeddedCassandraServerHelper.getCluster.getMetadata.getKeyspace(topicAndKeyspace).getName === topicAndKeyspace)
      // move to keyspace
      session.execute(s"USE $topicAndKeyspace;")
      // create table from cql
      val stream: InputStream = getClass.getResourceAsStream("/persister.cql")
      for (line <- Source.fromInputStream(stream).getLines) {
        if (!line.startsWith("//")) session.execute(line)
      }
    }

    // 2) start the driver asynchronously
    {
      es.submit(new Runnable {
        override def run() {
          Persister.main(Array[String]())
        }
      })
    }

    // 3) push signal to kafka
    {
      new TestSignalProducer(kafkaServer.getKafkaConnect, topicAndKeyspace, numberOfSignalsPerSignalId, List(100, 110),
        SORTER, "persisterProducer")
    }

    // 4) query cassandra until all the signals have been saved
    {
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
      // wait until we get a success, waiting 2 minute
      Await.ready(count, Duration.create(2, TimeUnit.MINUTES))
    }
  }
}