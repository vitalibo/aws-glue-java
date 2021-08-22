package com.github.vitalibo.glue;


import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.GlueArgParser;
import com.github.vitalibo.glue.api.java.JavaGlueContext;
import com.github.vitalibo.glue.util.YamlConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import scala.collection.JavaConversions;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
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

    public Job createJob(String[] args) {
        final Map<String, String> options = JavaConversions.mapAsJavaMap(
            GlueArgParser.getResolvedOptions(args, new String[]{"JOB_NAME"}));
        String jobName = parseJobName(options.get("JOB_NAME"));

        return internalCreateJob(config.asObject("Jobs." + jobName), args);
    }

    public Spark createSpark(String[] args) {
        final Map<String, String> options = JavaConversions.mapAsJavaMap(
            GlueArgParser.getResolvedOptions(args, new String[]{"JOB_NAME"}));

        SparkContext sparkContext = new SparkContext();
        GlueContext glueContext = new GlueContext(sparkContext);
        return new Spark(options, new JavaGlueContext(glueContext));
    }

    @SneakyThrows
    private Job internalCreateJob(YamlConfiguration config, String[] args) {
        final List<Object> constructorArgs = new ArrayList<>();
        for (YamlConfiguration conf : config.asListObjects("Args")) {
            Method method = Factory.class.getDeclaredMethod(
                String.format("create%s", conf.getString("Type")), YamlConfiguration.class, String[].class);
            constructorArgs.add(method.invoke(this, conf, args));
        }

        Class<?> cls = Class.forName(config.getString("ClassName"));
        for (Constructor<?> constructor : cls.getConstructors()) {
            try {
                return (Job) constructor.newInstance(constructorArgs.toArray());
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }
        }

        throw new IllegalArgumentException("Can't create job instance.");
    }

    private Source createSource(YamlConfiguration config, String[] args) {
        return (gc) -> gc.getSource(
            config.getString("ConnectionType"),
            config.getJsonOptions("ConnectionOptions"),
            config.getString("Format"),
            config.getJsonOptions("FormatOptions"),
            JavaGlueContext.kwargs()
                .transformationContext(config.getString("TransformationContext", "")));
    }

    private Sink createSink(YamlConfiguration config, String[] args) {
        return (gc) -> gc.getSink(
            config.getString("ConnectionType"),
            config.getJsonOptions("ConnectionOptions"),
            config.getString("Format"),
            config.getJsonOptions("FormatOptions"),
            JavaGlueContext.kwargs()
                .transformationContext(config.getString("TransformationContext", "")));
    }

    private String parseJobName(String glueJobName) {
        String fullName = Arrays.stream(glueJobName.split("-"))
            .map(o -> Character.toUpperCase(o.charAt(0)) + o.substring(1))
            .collect(Collectors.joining());

        @SuppressWarnings("unchecked")
        Set<String> supportedJobs = config.get("Jobs")
            .map(o -> ((Map<String, ?>) o).keySet()).orElse(new HashSet<>());
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
