package com.dematic.labs.dsp.drivers;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.dematic.labs.dsp.configuration.DriverConfiguration;
import com.dematic.labs.dsp.configuration.DriverUnitTestConfiguration;
import info.batey.kafka.unit.KafkaUnit;
import org.apache.thrift.transport.TTransportException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.cassandraunit.utils.EmbeddedCassandraServerHelper.*;

public final class SignalAggregationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SignalAggregationTest.class);
    private static final String KEYSPACE = "signal_aggregation";

    @Rule
    public TemporaryFolder checkpoint = new TemporaryFolder();

    private final DriverUnitTestConfiguration.Builder builder;
    private final KafkaUnit kafkaServer;

    public SignalAggregationTest() throws IOException, URISyntaxException {
        checkpoint.create();
        final URI uri = getClass().getResource("/signalAggregation.conf").toURI();
        builder = new DriverUnitTestConfiguration.Builder(Paths.get(uri).toFile());
        kafkaServer = new KafkaUnit();
    }

    @Before
    public void initialize() throws IOException, TTransportException {
        // 1) start kafka server and create topic, only one broker created during testing
        kafkaServer.setKafkaBrokerConfig("offsets.topic.replication.factor", "1");
        kafkaServer.startup();
        // 2) start cassandra and create keyspace/table
        startEmbeddedCassandra(CASSANDRA_RNDPORT_YML_FILE);
        LOGGER.info("kafka server = '{}' cassandra = 'localhost:{}'", kafkaServer.getKafkaConnect(),
                getNativeTransportPort());

        final DriverConfiguration config = builder.sparkCheckpointLocation(checkpoint.getRoot().getPath())
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
                session.execute(String.format("CREATE KEYSPACE if not exists %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };", KEYSPACE));
                Assert.assertEquals(KEYSPACE, cluster.getMetadata().getKeyspace(KEYSPACE).getName());
                // move to keyspace
                session.execute(String.format("USE %s", KEYSPACE));
                // create table from cql
                final URI uri = getClass().getResource("/SignalAggregation.cql").toURI();
                try (final Stream<String> stream =
                             Files.lines(Paths.get(uri)).filter(f -> !f.startsWith("//"))) {
                    stream.forEach(session::execute);
                }
            }
        }
        // 2) start the driver asynchronously

    }
}

