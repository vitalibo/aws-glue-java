package com.github.vitalibo.glue;


import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.GlueArgParser;
import com.github.vitalibo.glue.util.YamlConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class Factory {

    private final YamlConfiguration config;

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

        Class<?> cls = Class.forName(config.getString(String.format("Jobs.%s.ClassName", jobName)));
        return (Job) cls.newInstance();
    }

    public Spark createSpark(String[] args) {
        final Map<String, String> options = JavaConversions.mapAsJavaMap(
            GlueArgParser.getResolvedOptions(args, new String[]{"JOB_NAME"}));

        final JavaSparkContext javaSparkContext = new JavaSparkContext();
        final GlueContext glueContext = new GlueContext(javaSparkContext.sc());
        return new Spark(options, glueContext);
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

    public static Factory getInstance() {
        return new Factory(
            YamlConfiguration.parseResources("/default-application.yaml"),
            YamlConfiguration.parseResources("/application.yaml"));
    }

}
