package com.github.vitalibo.glue;


import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class Factory {

    private final YamlConfiguration config;

    Factory(YamlConfiguration... configs) {
        config = Arrays.stream(configs)
            .reduce(YamlConfiguration::withFallback)
            .orElseThrow(IllegalStateException::new);
    }

    public static Factory getInstance() {
        return new Factory(
            YamlConfiguration.parseResources("/default-application.yaml"),
            YamlConfiguration.parseResources("/application.yaml"));
    }

    public Job createJob(String[] args) {
        return spark -> {
            logger.info("Execute Job");
        };
    }

    public Spark createSpark(String[] args) {
        return new Spark();
    }

}
