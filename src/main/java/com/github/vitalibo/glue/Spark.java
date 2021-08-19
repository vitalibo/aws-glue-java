package com.github.vitalibo.glue;

import com.amazonaws.services.glue.util.Job;
import com.github.vitalibo.glue.api.java.JavaDynamicFrame;
import com.github.vitalibo.glue.api.java.JavaGlueContext;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

@RequiredArgsConstructor
public class Spark {

    public final JavaGlueContext jgc;
    public final JavaSparkContext jsc;
    public final SparkSession session;
    public final SparkConf conf;

    private final Map<String, String> options;

    public Spark(Map<String, String> options, JavaGlueContext gc) {
        this(gc,
            new JavaSparkContext(gc.getSparkSession().sparkContext()),
            gc.getSparkSession(),
            gc.getSparkSession().sparkContext().conf(),
            options);
    }

    public void submit(com.github.vitalibo.glue.Job job) {
        try {
            Job.init(options.get("JOB_NAME"), jgc.delegate, options);

            job.process(this);
            Job.commit();
        } catch (Exception e) {
            Job.reset();
        }
    }

    public JavaDynamicFrame extract(Source source) {
        return source.create(jgc)
            .getDynamicFrame();
    }

    public void load(Sink sink, JavaDynamicFrame frame) {
        sink.create(jgc)
            .writeDynamicFrame(frame);
    }

    public JavaDynamicFrame dynamicFrame(Dataset<Row> df) {
        return JavaDynamicFrame.from(df, jgc);
    }

    public int executorInstances() {
        return conf.getInt("spark.executor.instances", 1);
    }

    public int executorCores() {
        return conf.getInt("spark.executor.cores", 1);
    }

    public int totalCores() {
        return executorInstances() * executorCores();
    }

}
