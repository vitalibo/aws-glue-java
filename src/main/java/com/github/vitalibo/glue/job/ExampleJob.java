package com.github.vitalibo.glue.job;

import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.Sink;
import com.github.vitalibo.glue.Source;
import com.github.vitalibo.glue.Spark;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

@RequiredArgsConstructor
public class ExampleJob implements Job {

    private final Source peopleSource;
    private final Source departmentSource;
    private final Sink salarySink;

    @Override
    public void process(Spark spark) {
        Dataset<Row> departments = spark.readDF(departmentSource);

        Dataset<Row> salaries = spark.readDF(peopleSource)
            .filter(col("age").geq(30))
            .join(
                departments,
                col("deptId").equalTo(
                    col("id")))
            .groupBy(
                departments.col("name"),
                col("gender"))
            .agg(
                avg(col("salary")).as("avg"),
                max(col("age")).as("max"))
            .coalesce(spark.executorInstances());

        spark.writeDF(salarySink, salaries);
    }

}
