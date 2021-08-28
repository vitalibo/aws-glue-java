package com.github.vitalibo.glue;

import com.amazonaws.services.glue.GlueContext;
import com.github.vitalibo.glue.api.java.JavaDataSink;
import com.github.vitalibo.glue.api.java.JavaDataSource;
import com.github.vitalibo.glue.api.java.JavaDynamicFrame;
import com.github.vitalibo.glue.api.java.JavaGlueContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import scala.Function1;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.vitalibo.glue.util.ScalaConverters.function;

public class GlueSuiteBase {

    protected static Spark spark;

    @BeforeSuite
    public void setupSpark() {
        final SparkConf conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("test-job")
            .set("spark.sql.session.timeZone", "UTC");

        final Map<String, String> options = new HashMap<>();
        options.put("JOB_NAME", "test-job");

        final SparkContext sc = new SparkContext(conf);
        final JavaGlueContext jgc = new JavaGlueContext(new GlueContext(sc));

        spark = new Spark(options, jgc);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    public static Dataset<Row> createDataFrame(String resource) {
        return createDataFrame(resource,
            TestHelper.resourceAsString(
                resource.replace(".json", ".schema.json")));
    }

    public static Dataset<Row> createDataFrame(String resource, String schema) {
        return createDataFrame(resource, (StructType) StructType.fromJson(schema));
    }

    public static Dataset<Row> createDataFrame(String resource, StructType schema) {
        JavaRDD<String> jsonRDD = spark.jsc
            .wholeTextFiles(GlueSuiteBase.class
                    .getClassLoader()
                    .getResource(resource.startsWith("/") ? resource.substring(1) : resource)
                    .getFile(),
                1)
            .map(o -> o._2);

        DataFrameReader reader = spark.jgc
            .getSparkSession()
            .read()
            .option("multiLine", true)
            .option("mode", "PERMISSIVE");

        if (schema != null) {
            reader = reader.schema(schema);
        }

        return reader
            .json(jsonRDD)
            .repartition(spark.totalCores() * 2);
    }

    public static JavaDynamicFrame createDynamicFrame(String resource) {
        return spark.asDynF(
            createDataFrame(resource));
    }

    public static JavaDynamicFrame createDynamicFrame(String resource, String schema) {
        return spark.asDynF(
            createDataFrame(resource, schema));
    }

    public static JavaDynamicFrame createDynamicFrame(String resource, StructType schema) {
        return spark.asDynF(
            createDataFrame(resource, schema));
    }

    public static Source createSource(String resource) {
        return (o) -> new JavaDataSource(null) {
            @Override
            public JavaDynamicFrame getDynamicFrame() {
                return createDynamicFrame(resource);
            }
        };
    }

    public static Source createSource(String resource, String schema) {
        return (o) -> new JavaDataSource(null) {
            @Override
            public JavaDynamicFrame getDynamicFrame() {
                return createDynamicFrame(resource, schema);
            }
        };
    }

    public static Source createSource(String resource, StructType schema) {
        return (o) -> new JavaDataSource(null) {
            @Override
            public JavaDynamicFrame getDynamicFrame() {
                return createDynamicFrame(resource, schema);
            }
        };
    }

    public static Sink createSink(String resource) {
        return (o) -> new JavaDataSink(null) {
            @Override
            public void writeDynamicFrame(JavaDynamicFrame frame) {
                assertDataFrameEquals(frame.toDF(), createDataFrame(resource));
            }
        };
    }

    public static Sink createSink(String resource, String schema) {
        return (o) -> new JavaDataSink(null) {
            @Override
            public void writeDynamicFrame(JavaDynamicFrame frame) {
                assertDataFrameEquals(frame.toDF(), createDataFrame(resource, schema));
            }
        };
    }

    public static Sink createSink(String resource, StructType schema) {
        return (o) -> new JavaDataSink(null) {
            @Override
            public void writeDynamicFrame(JavaDynamicFrame frame) {
                assertDataFrameEquals(frame.toDF(), createDataFrame(resource, schema));
            }
        };
    }

    public static void assertDataFrameEquals(Dataset<Row> actual, Dataset<Row> expected) {
        assertDataFrameEquals(actual, expected, false);
    }

    public static void assertDataFrameEquals(Dataset<Row> actual, Dataset<Row> expected, boolean ignoreSchema) {
        final Function1<Dataset<Row>, Dataset<Row>> preparation =
            function((df) -> df
                .cache()
                .repartition(1)
                .sort(df.columns()[0], df.columns()));

        actual = actual.transform(preparation);
        expected = expected.transform(preparation);

        if (!ignoreSchema) {
            Assert.assertEquals(actual.schema(), expected.schema());
        }

        try {
            Assert.assertEquals(actual.collectAsList(), expected.collectAsList());
        } catch (AssertionError e) {
            Pattern compile = Pattern.compile("Lists differ at element \\[([0-9]+)]:.*");
            Matcher matcher = compile.matcher(e.getMessage());
            if (!matcher.matches()) {
                throw e;
            }

            int element = Integer.parseInt(matcher.group(1));
            Assert.assertEquals(
                actual.showString(element + 20, 0, false),
                expected.showString(element + 20, 0, false));
        }
    }

}
