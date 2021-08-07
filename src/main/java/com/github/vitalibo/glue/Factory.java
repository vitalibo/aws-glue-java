package com.github.vitalibo.glue;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class Factory {

    Factory(Config... configs) {
        Arrays.stream(configs)
            .reduce(Config::withFallback)
            .orElseThrow(IllegalStateException::new)
            .resolve();
    }

    public static Factory getInstance() {
        return new Factory(
            ConfigFactory.load(), ConfigFactory.parseResources("application.hocon"),
            ConfigFactory.parseResources("default-application.hocon"));
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
