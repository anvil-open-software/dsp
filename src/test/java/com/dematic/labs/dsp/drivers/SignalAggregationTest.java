package com.dematic.labs.dsp.drivers;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dematic.labs.dsp.configuration.DriverConfiguration;
import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration;
import com.dematic.labs.dsp.data.SignalType;
import com.dematic.labs.dsp.simulators.TestSignalProducer;
import com.jayway.awaitility.Awaitility;
import info.batey.kafka.unit.KafkaUnit;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.thrift.transport.TTransportException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.cassandraunit.utils.EmbeddedCassandraServerHelper.*;

public final class SignalAggregationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SignalAggregationTest.class);

    @Rule
    public final TemporaryFolder checkpoint = new TemporaryFolder();

    private DriverConfiguration config;
    private final KafkaUnit kafkaServer;

    public SignalAggregationTest() throws IOException {
        checkpoint.create();
        kafkaServer = new KafkaUnit();
    }

    @Before
    public void initialize() throws IOException, TTransportException, URISyntaxException {
        // 1) start kafka server and create topic, only one broker created during testing
        kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1");
        kafkaServer.startup();
        // 2) start cassandra and create keyspace/table
        startEmbeddedCassandra(CASSANDRA_RNDPORT_YML_FILE);
        LOGGER.info("kafka server = '{}' cassandra = 'localhost:{}'", kafkaServer.getKafkaConnect(),
                getNativeTransportPort());

        final URI uri = getClass().getResource("/signalAggregation.conf").toURI();
        final DriverUnitTestConfiguration.Builder builder = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile());
        config = builder.sparkCheckpointLocation(checkpoint.getRoot().getPath())
                .sparkCassandraConnectionHost("localhost")
                .sparkCassandraConnectionPort(Integer.toString(getNativeTransportPort()))
                .kafkaBootstrapServer(kafkaServer.getKafkaConnect())
                .build();
        SignalAggregation.setDriverConfiguration(config);
    }

    @After
    public void cleanup() {
        kafkaServer.shutdown();
    }

    @Test
    public void all() throws IOException, URISyntaxException {
        //1 )  create a cassandra cluster and connect and create keyspace and table
        try (final Cluster cluster = getCluster()) {
            try (final Session session = cluster.connect()) {
                // create keyspace
                session.execute(String.format("CREATE KEYSPACE if not exists %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
                        config.getCassandraKeyspace()));
                Assert.assertEquals(config.getCassandraKeyspace(),
                        cluster.getMetadata().getKeyspace(config.getCassandraKeyspace()).getName());
                // move to keyspace
                session.execute(String.format("USE %s", config.getCassandraKeyspace()));
                // create table from cql
                final URI uri = getClass().getResource("/signalAggregation.cql").toURI();
                try (final Stream<String> stream =
                             Files.lines(Paths.get(uri)).filter(f -> !f.startsWith("//"))) {
                    stream.forEach(session::execute);
                }
            }

            // 2) start the driver asynchronously
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                try {
                    SignalAggregation.main(null);
                } catch (final StreamingQueryException sqe) {
                    throw new RuntimeException("Unexpected Error:", sqe);
                }
            });

            final List<Object> ranges = new ArrayList<>();
            ranges.add(100);
            ranges.add(200);

            final Seq<Object> signalIdRange = JavaConverters.collectionAsScalaIterableConverter(ranges).asScala().toSeq();

            //noinspection unchecked
            new TestSignalProducer(kafkaServer.getKafkaConnect(), config.getKafkaTopics(), 100,
                    signalIdRange, SignalType.PICKER(), "signalAggregationProducer");
            // 4) query cassandra until all the signals have been aggregated
            // set the defaults timeouts
            Awaitility.setDefaultTimeout(2, TimeUnit.MINUTES);


            // poll cassandra until aggregation tables exists
            Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                    pollDelay(10, TimeUnit.SECONDS).await().
                    until(() -> {
                        try (final Session session = cluster.connect()) {
                            final Row row = session.execute(String.format("select * from %s.signal_aggregation;",
                                    config.getCassandraKeyspace())).one();
                            Assert.assertTrue(row != null);
                        }
                    });
        }
    }
}
