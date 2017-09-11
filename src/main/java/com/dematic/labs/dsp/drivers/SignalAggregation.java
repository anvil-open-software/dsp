package com.dematic.labs.dsp.drivers;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.monitor.spark.MonitorConsts;
import com.dematic.labs.analytics.monitor.spark.PrometheusStreamingQueryListener;
import com.dematic.labs.dsp.configuration.DefaultDriverConfiguration;
import com.dematic.labs.dsp.configuration.DriverConfiguration;
import com.google.common.base.Strings;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Symbol;

import static org.apache.spark.sql.types.DataTypes.*;
import static scala.compat.java8.JFunction.func;

/**
 * -Dconfig.file=path/to/file/signalAggregation.conf
 */
public final class SignalAggregation {
    // should only be  used with testing
    private static DriverConfiguration injectedDriverConfiguration;

    static void setDriverConfiguration(final DriverConfiguration driverConfiguration) {
        injectedDriverConfiguration = driverConfiguration;
    }

    public static void main(final String[] args) throws StreamingQueryException {
        final DriverConfiguration config = injectedDriverConfiguration != null ? injectedDriverConfiguration :
                new DefaultDriverConfiguration.Builder().build();
        // create the spark session
        final SparkSession.Builder builder = SparkSession.builder();
        final String masterUrl = config.getSparkMaster();
        if (!Strings.isNullOrEmpty(masterUrl)) {
            builder.master(masterUrl);
        }
        builder.appName(config.getDriverAppName());
        builder.config("spark.cassandra.connection.host", config.getSparkCassandraConnectionHost());
        builder.config("spark.cassandra.connection.port", config.getSparkCassandraConnectionPort());
        builder.config("spark.cassandra.auth.username", config.getSparkCassandraAuthUsername());
        builder.config("spark.cassandra.auth.password", config.getSparkCassandraAuthPassword());

        final SparkSession sparkSession = builder.getOrCreate();

        // hook up Prometheus listener for monitoring
        if (!Strings.isNullOrEmpty(System.getProperty(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY))) {
            sparkSession.streams().addListener(
                    new PrometheusStreamingQueryListener(sparkSession.sparkContext().getConf(),
                            config.getDriverAppName()));
        }

        // create the cassandra connector
        final CassandraConnector cassandraConnector = CassandraConnector.apply(sparkSession.sparkContext().getConf());

        try {
            final Dataset<Row> kafka = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config.getKafkaBootstrapServers())
                    .option(config.getKafkaSubscribe(), config.getKafkaTopics())
                    .option("startingOffsets", config.getKafkaStartingOffsets())
                    .option("maxOffsetsPerTrigger", config.getKafkaMaxOffsetsPerTrigger())
                    .load();

            // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset],
            // value schema is the following:
            final StructType schema = new StructType()
                    .add(new StructField("id", LongType, false, null))
                    .add(new StructField("timestamp", StringType, false, null))
                    .add(new StructField("value", IntegerType, false, null))
                    .add(new StructField("producerId", StringType, true, null));

            // retrieve signals as json
            final Dataset<Row> signals = kafka.selectExpr("cast (value as string) as json").
                    select(functions.from_json(new ColumnName("json"), schema).as("signals")).select("signals.*");

            // group by id and 5 minute intervals, and calculate stats
            final Dataset<Row> aggregate = signals.groupBy(functions.window(signals.col("timestamp"), "5 minutes")
                    .as(Symbol.apply("aggregate_time")), signals.col("id"))
                    .agg(
                            functions.count("id"),
                            functions.avg("value"),
                            functions.min("value"),
                            functions.max("value"),
                            functions.sum("value"));

            // write to cassandra
            final StreamingQuery query = aggregate.writeStream()
                    .option("spark.sql.streaming.checkpointLocation", config.getSparkCheckpointLocation())
                    .outputMode(OutputMode.Complete())
                    .queryName("signalAggregation")
                    .foreach(new ForeachWriter<Row>() {
                        @Override
                        public void process(final Row value) {
                            cassandraConnector.withSessionDo(func(session -> session.execute(cql(value))));
                        }

                        @Override
                        public void close(final Throwable errorOrNull) {
                        }

                        @Override
                        public boolean open(final long partitionId, final long version) {
                            return true;
                        }

                        private Statement cql(final Row row) {
                            final GenericRowWithSchema aggregate = row.getAs(0);
                            return QueryBuilder.update(config.getCassandraKeyspace(), "signal_aggregation")
                                    .with(QueryBuilder.set("count", row.getAs(2)))
                                    .and(QueryBuilder.set("avg", row.getAs(3)))
                                    .and(QueryBuilder.set("min", row.getAs(4)))
                                    .and(QueryBuilder.set("max", row.getAs(5)))
                                    .and(QueryBuilder.set("sum", row.getAs(6)))
                                    .where(QueryBuilder.eq("id", row.getAs(1)))
                                    .and(QueryBuilder.eq("aggregate", aggregate.getAs(0)));
                        }
                    }).start();
            query.awaitTermination();
        } finally {
            sparkSession.close();
        }
    }
}