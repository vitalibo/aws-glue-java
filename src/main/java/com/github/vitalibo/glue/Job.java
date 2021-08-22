package com.github.vitalibo.glue;

@FunctionalInterface
public interface Job {

    void process(Spark spark);

}
