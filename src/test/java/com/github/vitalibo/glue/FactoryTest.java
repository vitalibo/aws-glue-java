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

    private Factory spyFactory;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        spyFactory = Mockito.spy(new Factory(mockYamlConfiguration));
    }

    @Test
    public void testCreateJob() {
        Mockito.doReturn(new TestExampleJob()).when(spyFactory).createExampleJob(Mockito.any(), Mockito.any());
        Mockito.when(mockYamlConfiguration.get("Jobs"))
            .thenReturn(new HashMap<String, Object>() {{
                put("ExampleJob", null);
                put("TestJob", null);
            }});

        Job actual = spyFactory.createJob(new String[]{"--JOB_NAME", "dev-stack-example-job"});

        Assert.assertTrue(actual instanceof TestExampleJob);
    }

    public static class TestExampleJob implements Job {
        @Override
        public void process(Spark spark) {
        }
    }

}
