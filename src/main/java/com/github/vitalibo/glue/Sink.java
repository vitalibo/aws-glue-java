package com.github.vitalibo.glue;

import com.github.vitalibo.glue.api.java.JavaDataSink;
import com.github.vitalibo.glue.api.java.JavaGlueContext;

import java.util.function.Function;

@FunctionalInterface
public interface Sink extends Function<JavaGlueContext, JavaDataSink> {

    default JavaDataSink create(JavaGlueContext gc) {
        return apply(gc);
    }

}
