package com.github.vitalibo.glue;

import com.amazonaws.services.glue.util.Job;
import com.github.vitalibo.glue.api.java.JavaDataSink;
import com.github.vitalibo.glue.api.java.JavaDataSource;
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
            throw e;
        }
    }

    public JavaDataSource read(Source source) {
        return source.create(jgc);
    }

    public JavaDynamicFrame readDynF(Source source) {
        return read(source)
            .getDynamicFrame();
    }

    public Dataset<Row> readDF(Source source) {
        return readDynF(source)
            .toDF();
    }

    public JavaDataSink write(Sink sink) {
        return sink.create(jgc);
    }

    public void writeDynF(Sink sink, JavaDynamicFrame frame) {
        write(sink)
            .writeDynamicFrame(frame);
    }

    public void writeDF(Sink sink, Dataset<Row> df) {
        writeDynF(sink, asDynF(df));
    }

    public JavaDynamicFrame asDynF(Dataset<Row> df) {
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
