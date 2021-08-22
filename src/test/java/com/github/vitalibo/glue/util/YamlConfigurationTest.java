package com.github.vitalibo.glue.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

public class YamlConfigurationTest {

    @Test
    public void testParseResources() {
        YamlConfiguration actual = YamlConfiguration.parseResources("/application-test.yaml");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get("Parameters.Environment"), Optional.of("test"));
    }

    @Test
    public void testWithFallback() {
        YamlConfiguration actual = YamlConfiguration.parseResources("/application-default-test.yaml")
            .withFallback(YamlConfiguration.parseResources("/application-test.yaml"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getString("Parameters.Environment"), "test");
        Assert.assertEquals(actual.getString("Parameters.Name"), "ProjectName");
        Assert.assertEquals(actual.getString("Parameters.Bucket"), "S3Bucket");
        Assert.assertEquals(actual.getString("Jobs.TestJob.Class"), "com.github.vitalibo.glue.job.SampleJob");
        Assert.assertEquals(actual.getString("Jobs.TestJob.Name"), "SampleJob");
        Assert.assertEquals(actual.getInteger("Jobs.TestJob.ExecutorCores"), (Integer) 1);
        Assert.assertEquals(actual.getString("Jobs.FooJob.Class"), "com.github.vitalibo.glue.job.FooJob");
        Assert.assertEquals(actual.getString("Jobs.FooJob.Name"), "FooJob");
    }

}
