package com.dematic.labs.dsp.drivers.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.Serializable;

public abstract class DriverConfiguration implements Serializable {
    public static abstract class Builder {
        // Driver Keys
        private static final String DRIVER_APP_NAME_KEY = "driver.appName";
        // Spark Keys
        private static final String SPARK_MASTER_KEY = "spark.master";
        private static final String SPARK_CHECKPOINT_KEY = "spark.sql.streaming.checkpointLocation";
        private static final String SPARK_SHUFFLE_PARTITION_KEY = "spark.sql.shuffle.partition";
        private static final String SPARK_OUTPUT_MODE_KEY = "spark.output.mode";
        private static final String SPARK_WATERMARK_TIME_KEY = "spark.watermark.time";
        private static final String SPARK_QUERY_TRIGGER_KEY = "spark.query.trigger";
        private static final String SPARK_CASSANDRA_CONNECTION_HOST_KEY = "spark.cassandra.connection.host";
        private static final String SPARK_CASSANDRA_CONNECTION_PORT_KEY = "spark.cassandra.connection.port";
        private static final String SPARK_CASSANDRA_AUTH_USERNAME_KEY = "spark.cassandra.auth.username";
        private static final String SPARK_CASSANDRA_AUTH_PASSWORD_KEY = "spark.cassandra.auth.password";
        // Kafka Keys
        private static final String KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers";
        private static final String KAFKA_TOPICS_KEY = "kafka.topics";
        private static final String KAFKA_SUBSCRIBE_KEY = "kafka.subscribe";
        private static final String KAFKA_STARTING_OFFSETS_KEY = "kafka.startingOffsets";
        private static final String KAFKA_MAX_OFFSETS_PER_TRIGGER_KEY = "kafka.maxOffsetsPerTrigger";
        // Cassandra Keys
        private static final String CASSANDRA_KEYSPACE_KEY = "cassandra.keyspace";

        // configuration that holds values set from configuration file
        private Config config;
        // driver properties
        private String driverAppName;
        // spark properties
        private String sparkMaster;
        private String sparkCheckpointLocation;
        private String sparkShufflePartition;
        private String sparkOutputMode;
        private String sparkWatermarkTime;
        private String sparkQueryTrigger;
        private String sparkCassandraConnectionHost;
        private String sparkCassandraConnectionPort;
        private String sparkCassandraAuthUsername;
        private String sparkCassandraAuthPassword;
        // kafka properties
        private String kafkaBootstrapServers;
        private String kafkaTopics;
        private String kafkaSubscribe;
        private String kafkaStartingOffsets;
        private String kafkaMaxOffsetsPerTrigger;
        // cassandra properties
        private String cassandraKeyspace;

        Builder() {
            config = ConfigFactory.load();
            initializeConfigurationProperties();
        }

        // should be used only for testing
        Builder(final File resource) {
            config = ConfigFactory.parseFile(resource).withFallback(ConfigFactory.defaultReference());
            initializeConfigurationProperties();
        }

        private void initializeConfigurationProperties() {

            driverAppName = config.getString(DRIVER_APP_NAME_KEY);
            sparkMaster = config.getString(SPARK_MASTER_KEY);
            sparkCheckpointLocation = config.getString(SPARK_CHECKPOINT_KEY);
            sparkShufflePartition = config.getString(SPARK_SHUFFLE_PARTITION_KEY);
            sparkOutputMode = config.getString(SPARK_OUTPUT_MODE_KEY);
            sparkWatermarkTime = config.getString(SPARK_WATERMARK_TIME_KEY);
            sparkQueryTrigger = config.getString(SPARK_QUERY_TRIGGER_KEY);
            sparkCassandraConnectionHost = config.getString(SPARK_CASSANDRA_CONNECTION_HOST_KEY);
            sparkCassandraConnectionPort = config.getString(SPARK_CASSANDRA_CONNECTION_PORT_KEY);
            sparkCassandraAuthUsername = config.getString(SPARK_CASSANDRA_AUTH_USERNAME_KEY);
            sparkCassandraAuthPassword = config.getString(SPARK_CASSANDRA_AUTH_PASSWORD_KEY);
            kafkaBootstrapServers = config.getString(KAFKA_BOOTSTRAP_SERVERS_KEY);
            kafkaTopics = config.getString(KAFKA_TOPICS_KEY);
            kafkaSubscribe = config.getString(KAFKA_SUBSCRIBE_KEY);
            kafkaStartingOffsets = config.getString(KAFKA_STARTING_OFFSETS_KEY);
            kafkaMaxOffsetsPerTrigger = config.getString(KAFKA_MAX_OFFSETS_PER_TRIGGER_KEY);
            cassandraKeyspace = config.getString(CASSANDRA_KEYSPACE_KEY);

        }


        public abstract DriverConfiguration build();
    }

    // driver properties
    private final String driverAppName;
    // spark properties
    private final String sparkMaster;
    private final String sparkCheckpointLocation;
    private final String sparkShufflePartition;
    private final String sparkOutputMode;
    private final String sparkWatermarkTime;
    private final String sparkQueryTrigger;
    private final String sparkCassandraConnectionHost;
    private final String sparkCassandraConnectionPort;
    private final String sparkCassandraAuthUsername;
    private final String sparkCassandraAuthPassword;
    // kafka properties
    private final String kafkaBootstrapServers;
    private final String kafkaTopics;
    private final String kafkaSubscribe;
    private final String kafkaStartingOffsets;
    private final String kafkaMaxOffsetsPerTrigger;
    // cassandra properties
    private final String cassandraKeyspace;

    private Config config;

    DriverConfiguration(final DriverConfiguration.Builder builder) {
        driverAppName = builder.driverAppName;
        sparkMaster = builder.sparkMaster;
        sparkCheckpointLocation = builder.sparkCheckpointLocation;
        sparkShufflePartition = builder.sparkShufflePartition;
        sparkOutputMode = builder.sparkOutputMode;
        sparkWatermarkTime = builder.sparkWatermarkTime;
        sparkQueryTrigger = builder.sparkQueryTrigger;
        sparkCassandraConnectionHost = builder.sparkCassandraConnectionHost;
        sparkCassandraConnectionPort = builder.sparkCassandraConnectionPort;
        sparkCassandraAuthUsername = builder.sparkCassandraAuthUsername;
        sparkCassandraAuthPassword = builder.sparkCassandraAuthPassword;
        kafkaBootstrapServers = builder.kafkaBootstrapServers;
        kafkaTopics = builder.kafkaTopics;
        kafkaSubscribe = builder.kafkaSubscribe;
        kafkaStartingOffsets = builder.kafkaStartingOffsets;
        kafkaMaxOffsetsPerTrigger = builder.kafkaMaxOffsetsPerTrigger;
        cassandraKeyspace = builder.cassandraKeyspace;
        config = builder.config;
    }

    /**
     *
     * @param inKey
     * @return use for any config that is defined statically above
     */
    public String getConfigString(String inKey) {
        return config.getString(inKey);
    }

    public Long getConfigLong(String inKey) {
        return config.getLong(inKey);
    }

    public String getDriverAppName() {
        return driverAppName;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public String getSparkCheckpointLocation() {
        return sparkCheckpointLocation;
    }

    public String getSparkShufflePartition() {
        return sparkShufflePartition;
    }

    public String getSparkOutputMode() {
        return sparkOutputMode;
    }

    public String getSparkWatermarkTime() {
        return sparkWatermarkTime;
    }

    public String getSparkQueryTrigger() {
        return sparkQueryTrigger;
    }

    public String getSparkCassandraConnectionHost() {
        return sparkCassandraConnectionHost;
    }

    public String getSparkCassandraConnectionPort() {
        return sparkCassandraConnectionPort;
    }

    public String getSparkCassandraAuthUsername() {
        return sparkCassandraAuthUsername;
    }

    public String getSparkCassandraAuthPassword() {
        return sparkCassandraAuthPassword;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopics() {
        return kafkaTopics;
    }

    public String getKafkaSubscribe() {
        return kafkaSubscribe;
    }

    public String getKafkaStartingOffsets() {
        return kafkaStartingOffsets;
    }

    public String getKafkaMaxOffsetsPerTrigger() {
        return kafkaMaxOffsetsPerTrigger;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    @Override
    public String toString() {
        return "DriverConfiguration{" +
                "driverAppName='" + driverAppName + '\'' +
                ", sparkMaster='" + sparkMaster + '\'' +
                ", sparkCheckpointLocation='" + sparkCheckpointLocation + '\'' +
                ", sparkShufflePartition='" + sparkShufflePartition + '\'' +
                ", sparkOutputMode='" + sparkOutputMode + '\'' +
                ", sparkWatermarkTime='" + sparkWatermarkTime + '\'' +
                ", sparkQueryTrigger='" + sparkQueryTrigger + '\'' +
                ", sparkCassandraConnectionHost='" + sparkCassandraConnectionHost + '\'' +
                ", sparkCassandraConnectionPort='" + sparkCassandraConnectionPort + '\'' +
                ", sparkCassandraAuthUsername='" + sparkCassandraAuthUsername + '\'' +
                ", sparkCassandraAuthPassword='" + sparkCassandraAuthPassword + '\'' +
                ", kafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
                ", kafkaTopics='" + kafkaTopics + '\'' +
                ", kafkaSubscribe='" + kafkaSubscribe + '\'' +
                ", kafkaStartingOffsets='" + kafkaStartingOffsets + '\'' +
                ", kafkaMaxOffsetsPerTrigger='" + kafkaMaxOffsetsPerTrigger + '\'' +
                ", cassandraKeyspace='" + cassandraKeyspace + '\'' +
                '}';
    }
}