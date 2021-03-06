package com.github.vitalibo.glue.util;

import lombok.SneakyThrows;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple4;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public final class ScalaConverters {

    private ScalaConverters() {
    }

    public static <T1, T2> Tuple2<T1, T2> tuple(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }

    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> tuple(T1 t1, T2 t2, T3 t3, T4 t4) {
        return new Tuple4<>(t1, t2, t3, t4);
    }

    @SafeVarargs
    public static <T> Seq<T> seq(T... items) {
        return ScalaConverters.seq(Arrays.asList(items));
    }

    public static <T> Seq<T> seq(List<T> collection) {
        return JavaConversions.asScalaBuffer(collection)
            .toSeq();
    }

    public static <K> AbstractFunction0<K> supplier(Function0<K> supplier) {
        class Wraps extends AbstractFunction0<K> implements Serializable {
            @SneakyThrows
            public K apply() {
                return supplier.call();
            }
        }

        return new Wraps();
    }

    public static <T1, R> Function1<T1, R> function(Function<T1, R> function) {
        class Wraps extends AbstractFunction1<T1, R> implements Serializable {
            @SneakyThrows
            public R apply(T1 v1) {
                return function.call(v1);
            }
        }

        return new Wraps();
    }

}
