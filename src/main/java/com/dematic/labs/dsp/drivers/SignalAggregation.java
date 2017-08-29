package com.dematic.labs.dsp.drivers;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.monitor.spark.MonitorConsts;
import com.dematic.labs.analytics.monitor.spark.PrometheusStreamingQueryListener;
import com.google.common.base.Strings;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Symbol;

import static com.dematic.labs.dsp.configuration.DriverConfiguration.*;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * -Dconfig.file=path/to/file/signalAggregation.conf
 */
public class SignalAggregation {
    public static void main(final String[] args) {
        // create the spark session
        final SparkSession.Builder builder = SparkSession.builder();
        final String masterUrl = Spark$.MODULE$.masterUrl();
        if (!Strings.isNullOrEmpty(masterUrl)) {
            builder.master(masterUrl);
        }
        builder.appName(Driver$.MODULE$.appName());
        builder.config(Spark$.MODULE$.CassandraHostKey(), Spark$.MODULE$.cassandraHost());
        builder.config(Spark$.MODULE$.CassandraUsernameKey(), Spark$.MODULE$.cassandraUsername());
        builder.config(Spark$.MODULE$.CassandraPasswordKey(), Spark$.MODULE$.cassandraPassword());

        final SparkSession sparkSession = builder.getOrCreate();

        // hook up Prometheus listener for monitoring
        if (System.getProperties().contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
            sparkSession.streams().addListener(
                    new PrometheusStreamingQueryListener(sparkSession.sparkContext().getConf(),
                            Driver$.MODULE$.appName()));
        }

        // create the cassandra connector
        final CassandraConnector cassandraConnector = CassandraConnector.apply(sparkSession.sparkContext().getConf());

        try {
            final Dataset<Row> kafka = sparkSession.readStream()
                    .format(Kafka$.MODULE$.format())
                    .option(Kafka$.MODULE$.BootstrapServersKey(), Kafka$.MODULE$.bootstrapServers())
                    .option(Kafka$.MODULE$.subscribe(), Kafka$.MODULE$.topics())
                    .option(removeQualifier(Kafka$.MODULE$.StartingOffsetsKey()), Kafka$.MODULE$.startingOffsets())
                    .option(removeQualifier(Kafka$.MODULE$.MaxOffsetsPerTriggerKey()), Kafka$.MODULE$.maxOffsetsPerTrigger())
                    .load();

            // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset],
            // value schema is the following:
            final StructType schema = new StructType()
                    .add(new StructField("id", LongType, false, null))
                    .add(new StructField("timestamp", StringType, false, null))
                    .add(new StructField("value", IntegerType, false, null))
                    .add(new StructField("producerId", StringType, true, null));

            // convert to json and select all values
            final Dataset<Row> signals = kafka.selectExpr("cast (value as string) as json").
                    select(functions.from_json(new Column("json"), schema)).as("signals").select("signals.*");

            // todo: looking watermark time
            final Dataset<Row> aggregate = signals.groupBy(
                    functions.window(new Column("timestamp"), "5 minutes")
                            .as(Symbol.apply("aggregate_time")), new Column("opcTagId")).
                    agg(
                            functions.count("opcTagId"),
                            functions.avg("value"),
                            functions.max("value"),
                            functions.sum("value"));

            // write to cassandra

        } finally {
            sparkSession.close();
        }
    }
}