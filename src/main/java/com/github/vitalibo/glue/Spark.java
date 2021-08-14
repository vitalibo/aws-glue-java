package com.github.vitalibo.glue;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;

import java.util.Map;

@RequiredArgsConstructor
public class Spark {

    public final GlueContext gc;
    public final SparkContext sc;

    private final Map<String, String> options;

    public Spark(Map<String, String> options, GlueContext gc) {
        this(gc, gc.sc(), options);
    }

    public void submit(com.github.vitalibo.glue.Job job) {
        try {
            Job.init(options.get("JOB_NAME"), gc, options);

            job.process(this);
            Job.commit();
        } catch (Exception e) {
            Job.reset();
        }
    }

}
