package com.github.vitalibo.glue.job;

import com.amazonaws.services.glue.DynamicRecord;
import com.amazonaws.services.glue.types.StringNode;
import com.github.vitalibo.glue.Job;
import com.github.vitalibo.glue.Sink;
import com.github.vitalibo.glue.Source;
import com.github.vitalibo.glue.Spark;
import com.github.vitalibo.glue.api.java.JavaDynamicFrame;
import lombok.RequiredArgsConstructor;
import scala.Option;

import java.util.Objects;
import java.util.function.Function;

import static com.github.vitalibo.glue.util.ScalaConverters.function;
import static com.github.vitalibo.glue.util.ScalaConverters.tuple;

@RequiredArgsConstructor
public class DataCleaningJob implements Job {

    private final Source medicareSource;
    private final Sink medicareSink;

    @Override
    public void process(Spark spark) {
        JavaDynamicFrame medicareDynF = spark.readDynF(medicareSource)
            .resolveChoice(tuple("Provider Id", "cast:long"))
            .filter(o -> o.getField("Provider Id").exists(function(Objects::nonNull)))
            .map(o -> chopFirst("Average Covered Charges", "ACC").andThen(
                chopFirst("Average Total Payments", "ATP")).andThen(
                chopFirst("Average Medicare Payments", "AMP")).apply(o));

        medicareDynF.printSchema();
        System.out.printf("count: %s errors: %s\n", medicareDynF.count(), medicareDynF.errorsCount());
        medicareDynF.errorsAsDynamicFrame().show();

        JavaDynamicFrame medicareNest = medicareDynF.applyMapping(
            tuple("DRG Definition", "string", "drg", "string"),
            tuple("Provider Id", "long", "provider.id", "long"),
            tuple("Provider Name", "string", "provider.name", "string"),
            tuple("Provider City", "string", "provider.city", "string"),
            tuple("Provider State", "string", "provider.state", "string"),
            tuple("Provider Zip Code", "long", "provider.zip", "long"),
            tuple("Hospital Referral Region Description", "string", "rr", "string"),
            tuple("ACC", "string", "charges.covered", "double"),
            tuple("ATP", "string", "charges.total_pay", "double"),
            tuple("AMP", "string", "charges.medicare_pay", "double"));

        spark.writeDynF(medicareSink, medicareNest);
    }

    private static Function<DynamicRecord, DynamicRecord> chopFirst(String col, String newCol) {
        return rec -> {
            Option<Object> field = rec.getField(col);
            if (field.isDefined()) {
                rec.addField(newCol, new StringNode(((String) field.get()).substring(1)));
            }

            return rec;
        };
    }

}
