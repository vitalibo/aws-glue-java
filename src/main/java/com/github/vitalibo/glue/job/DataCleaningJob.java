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

import static com.github.vitalibo.glue.util.ScalaConverters.tuple;

@RequiredArgsConstructor
public class DataCleaningJob implements Job {

    private final Source medicareSource;
    private final Sink medicareSink;

    @Override
    public void process(Spark spark) {
        JavaDynamicFrame medicareDynF = spark.readDynF(medicareSource)
            .resolveChoice(tuple("provider id", "cast:long"))
            .filter(o -> o.getField("provider id").exists(Objects::nonNull))
            .map(chopFirst("average covered charges", "ACC").andThen(
                chopFirst("average total payments", "ATP")).andThen(
                chopFirst("average medicare payments", "AMP")));

        medicareDynF.printSchema();
        System.out.printf("count: %s errors: %s", medicareDynF.count(), medicareDynF.errorsCount());
        medicareDynF.errorsAsDynamicFrame().show();

        JavaDynamicFrame medicareNest = medicareDynF.applyMapping(
            tuple("drg definition", "string", "drg", "string"),
            tuple("provider id", "long", "provider.id", "long"),
            tuple("provider name", "string", "provider.name", "string"),
            tuple("provider city", "string", "provider.city", "string"),
            tuple("provider state", "string", "provider.state", "string"),
            tuple("provider zip code", "long", "provider.zip", "long"),
            tuple("hospital referral region description", "string", "rr", "string"),
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
