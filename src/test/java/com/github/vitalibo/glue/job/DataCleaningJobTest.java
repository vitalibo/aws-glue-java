package com.github.vitalibo.glue.job;

import com.github.vitalibo.glue.GlueSuiteBase;
import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.TestHelper;
import org.testng.annotations.Test;

public class DataCleaningJobTest extends GlueSuiteBase {

    @Test
    public void testProcess() {
        Job job = new DataCleaningJob(
            createSource(TestHelper.resourcePath("Medicare_Hospital_Provider.json")),
            createSink(TestHelper.resourcePath("Medicare_Cleaned.json")));

        job.process(spark);
    }

}
