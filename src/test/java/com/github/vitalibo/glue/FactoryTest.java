package com.github.vitalibo.glue;

import com.github.vitalibo.glue.util.YamlConfiguration;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

public class FactoryTest {

    @Mock
    private YamlConfiguration mockYamlConfiguration;

    private Factory factory;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        factory = new Factory(mockYamlConfiguration);
    }

    @Test
    public void testCreateJob() {
        Mockito.when(mockYamlConfiguration.getString(Mockito.anyString()))
            .thenReturn("com.github.vitalibo.glue.FactoryTest$TestJob");
        Mockito.when(mockYamlConfiguration.get("Jobs"))
            .thenReturn(new HashMap<String, Object>() {{
                put("ExampleJob", null);
                put("TestJob", null);
            }});

        Job actual = factory.createJob(new String[]{"--JOB_NAME", "dev-stack-test-job"});

        Assert.assertTrue(actual instanceof TestJob);
        Mockito.verify(mockYamlConfiguration).getString("Jobs.TestJob.ClassName");
    }

    public static class TestJob implements Job {
        @Override
        public void process(Spark spark) {
        }
    }

}
