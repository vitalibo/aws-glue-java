package com.github.vitalibo.glue;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.github.vitalibo.glue.api.java.JavaGlueContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class SparkTest {

    @Mock
    private GlueContext mockGlueContext;
    @Mock
    private JavaSparkContext mockJavaSparkContext;
    @Mock
    private SparkSession mockSparkSession;
    @Mock
    private SparkConf mockSparkConf;
    @Mock
    private com.github.vitalibo.glue.Job mockJob;

    private MockedStatic<Job> mockStaticJob;
    private Map<String, String> options;
    private Spark spark;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        options = new HashMap<>();
        options.put("JOB_NAME", "foo-bar-baz");
        spark = new Spark(new JavaGlueContext(mockGlueContext), mockJavaSparkContext, mockSparkSession, mockSparkConf, options);
        mockStaticJob = Mockito.mockStatic(Job.class);
    }

    @Test
    public void testSubmit() {
        spark.submit(mockJob);

        mockStaticJob.verify(() -> Job.init("foo-bar-baz", mockGlueContext, options));
        mockStaticJob.verify(Job::commit);
        mockStaticJob.verify(Mockito.never(), Job::reset);
    }

    @Test
    public void testSubmitJobFailed() {
        Mockito.doThrow(RuntimeException.class).when(mockJob).process(spark);

        spark.submit(mockJob);

        mockStaticJob.verify(() -> Job.init("foo-bar-baz", mockGlueContext, options));
        mockStaticJob.verify(Mockito.never(), Job::commit);
        mockStaticJob.verify(Job::reset);
    }

    @AfterMethod
    public void cleanUp() {
        mockStaticJob.close();
    }

}
