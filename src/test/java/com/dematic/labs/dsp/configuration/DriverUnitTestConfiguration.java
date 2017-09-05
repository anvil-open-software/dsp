package com.dematic.labs.dsp.configuration;

import java.io.File;

public final class DriverUnitTestConfiguration extends DriverConfiguration {
    public static class Builder extends DriverConfiguration.Builder {
        // dynamic overridden values used in testing
        private String sparkCheckpointLocation;
        private String sparkCassandraConnectionHost;
        private String kafkaBootstrapServer;

        public Builder(final File resource) {
            super(resource);
        }

        public Builder sparkCheckpointLocation(final String sparkCheckpointLocation) {
            this.sparkCheckpointLocation = sparkCheckpointLocation;
            return this;
        }

        public Builder sparkCassandraConnectionHost(final String sparkCassandraConnectionHost) {
            this.sparkCassandraConnectionHost = sparkCassandraConnectionHost;
            return this;
        }

        public Builder kafkaBootstrapServer(final String kafkaBootstrapServer) {
            this.kafkaBootstrapServer = kafkaBootstrapServer;
            return this;
        }

        public DriverUnitTestConfiguration build() {
            return new DriverUnitTestConfiguration(this);
        }
    }

    private final String sparkCheckpointLocation;
    private final String sparkCassandraConnectionHost;
    private final String kafkaBootstrapServer;

    DriverUnitTestConfiguration(final Builder builder) {
        super(builder);
        // if not overridden, get defaults
        sparkCheckpointLocation = builder.sparkCheckpointLocation != null ? builder.sparkCheckpointLocation :
                super.getSparkCheckpointLocation();
        sparkCassandraConnectionHost = builder.sparkCassandraConnectionHost != null ?
                builder.sparkCassandraConnectionHost : super.getSparkCassandraConnectionHost();
        kafkaBootstrapServer = builder.kafkaBootstrapServer != null ? builder.kafkaBootstrapServer :
                super.getKafkaBootstrapServers();
    }

    @Override
    public String getSparkCheckpointLocation() {
        return sparkCheckpointLocation;
    }

    @Override
    public String getSparkCassandraConnectionHost() {
        return sparkCassandraConnectionHost;
    }

    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }
}
