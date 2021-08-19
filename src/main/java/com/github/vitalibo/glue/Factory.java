package com.github.vitalibo.glue;


import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.GlueArgParser;
import com.github.vitalibo.glue.api.java.JavaGlueContext;
import com.github.vitalibo.glue.job.ExampleJob;
import com.github.vitalibo.glue.util.YamlConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import scala.collection.JavaConversions;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class Factory {

    private final YamlConfiguration config;

    public static Factory getInstance() {
        return new Factory(
            YamlConfiguration.parseResources("/default-application.yaml"),
            YamlConfiguration.parseResources("/application.yaml"));
    }

    Factory(YamlConfiguration... configs) {
        this(Arrays.stream(configs)
            .reduce(YamlConfiguration::withFallback)
            .orElseThrow(IllegalStateException::new));
    }

    @SneakyThrows
    public Job createJob(String[] args) {
        final Map<String, String> options = JavaConversions.mapAsJavaMap(
            GlueArgParser.getResolvedOptions(args, new String[]{"JOB_NAME"}));
        String jobName = parseJobName(options.get("JOB_NAME"));

        Method method = Factory.class.getDeclaredMethod(
            String.format("create%s", jobName), YamlConfiguration.class, String[].class);
        return (Job) method.invoke(this, config.subconfig("Jobs." + jobName), args);
    }

    public Spark createSpark(String[] args) {
        final Map<String, String> options = JavaConversions.mapAsJavaMap(
            GlueArgParser.getResolvedOptions(args, new String[]{"JOB_NAME"}));

        SparkContext sparkContext = new SparkContext();
        GlueContext glueContext = new GlueContext(sparkContext);
        return new Spark(options, new JavaGlueContext(glueContext));
    }

    public Job createExampleJob(YamlConfiguration config, String[] args) {
        return new ExampleJob(
            createSource("PeopleSource", config),
            createSource("DepartmentSource", config),
            createSink("SalarySink", config));
    }

    private Source createSource(String context, YamlConfiguration config) {
        return (gc) -> gc.getSource(
            config.getString(String.format("%s.ConnectionType", context)),
            config.getJsonOptions(String.format("%s.ConnectionOptions", context)),
            config.getString(String.format("%s.Format", context)),
            config.getJsonOptions(String.format("%s.FormatOptions", context)),
            JavaGlueContext.kwargs()
                .transformationContext(context));
    }

    private Sink createSink(String context, YamlConfiguration config) {
        return (gc) -> gc.getSink(
            config.getString(String.format("%s.ConnectionType", context)),
            config.getJsonOptions(String.format("%s.ConnectionOptions", context)),
            config.getString(String.format("%s.Format", context)),
            config.getJsonOptions(String.format("%s.FormatOptions", context)),
            JavaGlueContext.kwargs()
                .transformationContext(context));
    }

    private String parseJobName(String glueJobName) {
        String fullName = Arrays.stream(glueJobName.split("-"))
            .map(o -> Character.toUpperCase(o.charAt(0)) + o.substring(1))
            .collect(Collectors.joining());

        @SuppressWarnings("unchecked")
        Set<String> supportedJobs = ((Map<String, ?>) config.get("Jobs")).keySet();
        for (int i = 0; i < fullName.length(); i++) {
            String subName = fullName.substring(i);
            if (supportedJobs.contains(subName)) {
                return subName;
            }
        }

        throw new IllegalArgumentException(
            String.format("Not found configuration for job '%s'", glueJobName));
    }

}
