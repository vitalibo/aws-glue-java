package com.github.vitalibo.glue.job;

import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.Sink;
import com.github.vitalibo.glue.Source;
import com.github.vitalibo.glue.Spark;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;

@RequiredArgsConstructor
public class ExampleJob implements Job {

    private final Source peopleSource;
    private final Source departmentSource;
    private final Sink salarySink;

    @Override
    public void process(Spark spark) {
        Dataset<Row> peoples = spark.extract(peopleSource).toDF();
        Dataset<Row> departments = spark.extract(departmentSource).toDF();

        Dataset<Row> salaries = peoples
            .filter(peoples.col("age").geq(30))
            .join(
                spark.extract(departmentSource).toDF(),
                peoples.col("deptId")
                    .equalTo(departments.col("id")))
            .groupBy(
                departments.col("name"), peoples.col("gender"))
            .agg(
                avg(departments.col("salary")),
                max(peoples.col("age")));

        spark.load(salarySink, spark.dynamicFrame(salaries));
    }

}
