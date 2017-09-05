package com.dematic.labs.dsp.configuration;

/**
 * Will load all properties from application.conf file set using system properties.
 *
 *  -Dconfig.file=path/to/file/application.conf
 */
public final class DefaultDriverConfiguration extends DriverConfiguration {
    public static class Builder extends DriverConfiguration.Builder {
        public DefaultDriverConfiguration build() {
            return new DefaultDriverConfiguration(this);
        }
    }

    private DefaultDriverConfiguration(final Builder builder) {
        super(builder);
    }
}
