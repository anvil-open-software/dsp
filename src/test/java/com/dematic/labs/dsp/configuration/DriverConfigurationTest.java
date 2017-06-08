package com.dematic.labs.dsp.configuration;

import org.junit.Assert;
import org.junit.Test;

import static com.dematic.labs.dsp.configuration.DriverConfiguration.*;

public class DriverConfigurationTest {
    
    @Test
    public void overrideConfiguration() {
        // driver properties
        // from application.conf
        Assert.assertEquals("CumulativeCount", Driver$.MODULE$.appName());

        // spark properties
        // from application.conf
        Assert.assertEquals("local[*]", Spark$.MODULE$.masterUrl());
        // from application.conf
        Assert.assertEquals("/tmp/checkpoint", Spark$.MODULE$.checkpointLocation());
        // from reference.conf
        Assert.assertEquals("0 seconds", Spark$.MODULE$.queryTrigger());

        // kafka properties
        // from application.conf
        Assert.assertEquals("localhost:9092", Kafka$.MODULE$.bootstrapServers());
        // from application.conf
        Assert.assertEquals("test", Kafka$.MODULE$.topics());
    }
}
