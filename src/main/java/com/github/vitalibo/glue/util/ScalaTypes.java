package com.github.vitalibo.glue.util;

import scala.Function1;
import scala.Tuple2;
import scala.Tuple4;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;


public final class ScalaTypes {

    private ScalaTypes() {
    }

    public static <T1, T2> Tuple2<T1, T2> tuple(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }

    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> tuple(T1 t1, T2 t2, T3 t3, T4 t4) {
        return new Tuple4<>(t1, t2, t3, t4);
    }

    @SafeVarargs
    public static <T> Seq<T> seq(T... items) {
        return ScalaTypes.seq(Arrays.asList(items));
    }

    public static <T> Seq<T> seq(Iterable<T> iterable) {
        return JavaConverters.asScalaIteratorConverter(iterable.iterator())
            .asScala()
            .toSeq();
    }

    public static <K> AbstractFunction0<K> supplier(Supplier<K> supplier) {
        return new AbstractFunction0<K>() {
            @Override
            public K apply() {
                return supplier.get();
            }
        };
    }

    public static <T1, R> Function1<T1, R> function(Function<T1, R> function) {
        return new AbstractFunction1<T1, R>() {
            @Override
            public R apply(T1 v1) {
                return function.apply(v1);
            }
        };
    }

}
