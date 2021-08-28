package com.github.vitalibo.glue.job;

import com.github.vitalibo.glue.GlueSuiteBase;
import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.TestHelper;
import org.testng.annotations.Test;

public class IncomeAvgJobTest extends GlueSuiteBase {

    @Test
    public void testProcess() {
        Job job = new IncomeAvgJob(
            createSource(TestHelper.resourcePath("people.json")),
            createSource(TestHelper.resourcePath("department.json")),
            createSink(TestHelper.resourcePath("salary.json")));

        job.process(spark);
    }
}
