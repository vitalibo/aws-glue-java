package com.github.vitalibo.glue;

import com.github.vitalibo.glue.api.java.JavaDataSource;
import com.github.vitalibo.glue.api.java.JavaGlueContext;

import java.util.function.Function;

@FunctionalInterface
public interface Source extends Function<JavaGlueContext, JavaDataSource> {

    default JavaDataSource create(JavaGlueContext gc) {
        return apply(gc);
    }

}
