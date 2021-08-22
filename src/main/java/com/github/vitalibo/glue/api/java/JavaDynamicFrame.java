package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.ChoiceOption;
import com.amazonaws.services.glue.DynamicFrame;
import com.amazonaws.services.glue.DynamicRecord;
import com.amazonaws.services.glue.ResolveSpec;
import com.amazonaws.services.glue.errors.CallSite;
import com.amazonaws.services.glue.schema.Schema;
import com.amazonaws.services.glue.util.JsonOptions;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Option;
import scala.Product2;
import scala.Product4;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.github.vitalibo.glue.util.ScalaConverters.*;

@SuppressWarnings(value = {"PMD.ExcessivePublicCount", "PMD.ExcessiveClassLength", "PMD.GodClass", "PMD.ExcessiveParameterList"})
@RequiredArgsConstructor
public class JavaDynamicFrame {

    public final DynamicFrame delegate;

    public long errorsCount() {
        return delegate.errorsCount();
    }

    @SafeVarargs
    public final JavaDynamicFrame applyMapping(Product4<String, String, String, String>... mappings) {
        return applyMapping(mappings, true);
    }

    public JavaDynamicFrame applyMapping(Product4<String, String, String, String>[] mappings, boolean caseSensitive) {
        return applyMapping(mappings, caseSensitive, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame applyMapping(Product4<String, String, String, String>[] mappings, Kwargs kwargs) {
        return applyMapping(mappings, true, kwargs);
    }

    public JavaDynamicFrame applyMapping(Product4<String, String, String, String>[] mappings, boolean caseSensitive, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.applyMapping(
                seq(mappings),
                caseSensitive,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public void assertErrorThreshold() {
        delegate.assertErrorThreshold();
    }

    public long count() {
        return delegate.count();
    }

    public JavaDynamicFrame dropField(String path) {
        return dropField(path, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame dropField(String path, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.dropField(
                path,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame dropFields(String... fieldNames) {
        return dropFields(fieldNames, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame dropFields(String[] fieldNames, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.dropFields(
                seq(fieldNames),
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame dropNulls() {
        return dropNulls(JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame dropNulls(Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.dropNulls(
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame errorsAsDynamicFrame() {
        return new JavaDynamicFrame(
            delegate.errorsAsDynamicFrame());
    }

    public JavaDynamicFrame filter(Predicate<DynamicRecord> predicate) {
        return filter(predicate, "", JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame filter(Predicate<DynamicRecord> predicate, String errorMsg) {
        return filter(predicate, errorMsg, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame filter(Predicate<DynamicRecord> predicate, Kwargs kwargs) {
        return filter(predicate, "", kwargs);
    }

    public JavaDynamicFrame filter(Predicate<DynamicRecord> predicate, String errorMsg, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.filter(
                function(predicate::test),
                errorMsg,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public String getName() {
        return delegate.getName();
    }

    public int getNumPartitions() {
        return delegate.getNumPartitions();
    }

    public Optional<Schema> getSchemaIfComputed() {
        Option<Schema> option = delegate.getSchemaIfComputed();
        if (option.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(
            delegate.getSchemaIfComputed()
                .get());
    }

    public boolean isSchemaComputed() {
        return delegate.isSchemaComputed();
    }

    public JavaDynamicFrame join(JavaDynamicFrame frame2, String... keys) {
        return join(frame2, keys, keys, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame join(JavaDynamicFrame frame2, String[] keys, Kwargs kwargs) {
        return join(frame2, keys, keys, kwargs);
    }

    public JavaDynamicFrame join(JavaDynamicFrame frame2, String[] keys1, String[] keys2) {
        return join(frame2, keys1, keys2, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame join(JavaDynamicFrame frame2, String[] keys1, String[] keys2, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.join(
                seq(keys1),
                seq(keys2),
                frame2.delegate,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame map(Function<DynamicRecord, DynamicRecord> function) {
        return map(function, "");
    }

    public JavaDynamicFrame map(Function<DynamicRecord, DynamicRecord> function, String errorMsg) {
        return map(function, errorMsg, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame map(Function<DynamicRecord, DynamicRecord> function, Kwargs kwargs) {
        return map(function, "", kwargs);
    }

    public JavaDynamicFrame map(Function<DynamicRecord, DynamicRecord> function, String errorMsg, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.map(
                function(function::apply),
                errorMsg,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame mergeDynamicFrames(JavaDynamicFrame stageDynamicFrame, String... primaryKeys) {
        return mergeDynamicFrames(stageDynamicFrame, primaryKeys, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame mergeDynamicFrames(JavaDynamicFrame stageDynamicFrame, String[] primaryKeys, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.mergeDynamicFrames(
                stageDynamicFrame.delegate,
                seq(primaryKeys),
                kwargs.transformationContext,
                kwargs.options,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public void printSchema() {
        delegate.printSchema();
    }

    public Schema recomputeSchema() {
        return delegate.recomputeSchema();
    }

    public List<JavaDynamicFrame> relationalize(String rootTableName, String stagingPath) {
        return relationalize(rootTableName, stagingPath, JavaDynamicFrame.kwargs());
    }

    public List<JavaDynamicFrame> relationalize(String rootTableName, String stagingPath, Kwargs kwargs) {
        return JavaConverters.seqAsJavaListConverter(
                delegate.relationalize(
                    rootTableName,
                    stagingPath,
                    kwargs.options,
                    kwargs.transformationContext,
                    kwargs.callSite,
                    kwargs.stageThreshold,
                    kwargs.totalThreshold))
            .asJava()
            .stream()
            .map(JavaDynamicFrame::new)
            .collect(Collectors.toList());
    }

    public JavaDynamicFrame renameField(String oldName, String newName) {
        return renameField(oldName, newName, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame renameField(String oldName, String newName, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.renameField(
                oldName,
                newName,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame repartition(int numPartitions) {
        return repartition(numPartitions, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame repartition(int numPartitions, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.repartition(
                numPartitions,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    @SafeVarargs
    public final JavaDynamicFrame resolveChoice(Product2<String, String>... specs) {
        return resolveChoice(specs, null, null, null, null, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame resolveChoice(Product2<String, String>[] specs, Kwargs kwargs) {
        return resolveChoice(specs, null, null, null, null, kwargs);
    }

    public JavaDynamicFrame resolveChoice(Product2<String, String>[] specs, ChoiceOption choiceOption) {
        return resolveChoice(specs, choiceOption, null, null, null, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame resolveChoice(Product2<String, String>[] specs, ChoiceOption choiceOption, Kwargs kwargs) {
        return resolveChoice(specs, choiceOption, null, null, null, kwargs);
    }

    public JavaDynamicFrame resolveChoice(Product2<String, String>[] specs, ChoiceOption choiceOption, String database, String tableName, String catalogId) {
        return resolveChoice(specs, choiceOption, database, tableName, catalogId, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame resolveChoice(Product2<String, String>[] specs, ChoiceOption choiceOption, String database, String tableName, String catalogId, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.resolveChoice(
                seq(specs),
                Option.apply(choiceOption),
                Option.apply(database),
                Option.apply(tableName),
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold,
                Option.apply(catalogId)));
    }

    public Schema schema() {
        return delegate.schema();
    }

    public JavaDynamicFrame selectField(String fieldName) {
        return selectField(fieldName, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame selectField(String fieldName, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.selectField(
                fieldName,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame selectFields(String... paths) {
        return selectFields(paths, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame selectFields(String[] paths, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.selectFields(
                seq(paths),
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public void show() {
        show(20);
    }

    public void show(int numRows) {
        delegate.show(numRows);
    }

    public JavaDynamicFrame spigot(String path) {
        return spigot(path, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame spigot(String path, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.spigot(
                path,
                kwargs.options,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public List<JavaDynamicFrame> splitFields(String... paths) {
        return splitFields(paths, JavaDynamicFrame.kwargs());
    }

    public List<JavaDynamicFrame> splitFields(String[] paths, Kwargs kwargs) {
        return JavaConverters.seqAsJavaListConverter(
                delegate.splitFields(
                    seq(paths),
                    kwargs.transformationContext,
                    kwargs.callSite,
                    kwargs.stageThreshold,
                    kwargs.totalThreshold))
            .asJava()
            .stream()
            .map(JavaDynamicFrame::new)
            .collect(Collectors.toList());
    }

    public List<JavaDynamicFrame> splitRows(String[] paths, Object[] values, String[] operators) {
        return splitRows(paths, values, operators, JavaDynamicFrame.kwargs());
    }

    public List<JavaDynamicFrame> splitRows(String[] paths, Object[] values, String[] operators, Kwargs kwargs) {
        return JavaConverters.seqAsJavaListConverter(
                delegate.splitRows(
                    seq(paths),
                    seq(values),
                    seq(operators),
                    kwargs.transformationContext,
                    kwargs.callSite,
                    kwargs.stageThreshold,
                    kwargs.totalThreshold))
            .asJava()
            .stream()
            .map(JavaDynamicFrame::new)
            .collect(Collectors.toList());
    }

    public long stageErrorsCount() {
        return delegate.stageErrorsCount();
    }

    public Dataset<Row> toDF(ResolveSpec... specs) {
        return delegate.toDF(seq(specs));
    }

    public JavaDynamicFrame unbox(String path, String format) {
        return unbox(path, format, "{}", JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame unbox(String path, String format, Kwargs kwargs) {
        return unbox(path, format, "{}", kwargs);
    }

    public JavaDynamicFrame unbox(String path, String format, String optionString) {
        return unbox(path, format, optionString, JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame unbox(String path, String format, String optionString, Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.unbox(
                path,
                format,
                optionString,
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame unnest() {
        return unnest(JavaDynamicFrame.kwargs());
    }

    public JavaDynamicFrame unnest(Kwargs kwargs) {
        return new JavaDynamicFrame(
            delegate.unnest(
                kwargs.transformationContext,
                kwargs.callSite,
                kwargs.stageThreshold,
                kwargs.totalThreshold));
    }

    public JavaDynamicFrame withFrameSchema(Supplier<Schema> supplier) {
        return new JavaDynamicFrame(
            delegate.withFrameSchema(
                supplier(supplier)));
    }

    public JavaDynamicFrame withName(String name) {
        return new JavaDynamicFrame(
            delegate.withName(name));
    }

    public JavaDynamicFrame withTransformationContext(String name) {
        return new JavaDynamicFrame(
            delegate.withTransformationContext(name));
    }

    public static JavaDynamicFrame from(Dataset<Row> df, JavaGlueContext gc) {
        return new JavaDynamicFrame(
            DynamicFrame.apply(df, gc.delegate));
    }

    public static JavaDynamicFrame emptyDynamicFrame(JavaGlueContext gc) {
        return new JavaDynamicFrame(
            DynamicFrame.emptyDynamicFrame(gc.delegate));
    }

    public static Kwargs kwargs() {
        return new Kwargs(
            "",
            new CallSite("Not provided", ""),
            0,
            0,
            JsonOptions.empty());
    }

    @SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
    @AllArgsConstructor
    public static class Kwargs {

        String transformationContext;
        CallSite callSite;
        long stageThreshold;
        long totalThreshold;
        JsonOptions options;

        public Kwargs transformationContext(String transformationContext) {
            return new Kwargs(
                transformationContext,
                this.callSite,
                this.stageThreshold,
                this.totalThreshold,
                this.options);
        }

        public Kwargs callSite(CallSite callSite) {
            return new Kwargs(
                this.transformationContext,
                callSite,
                this.stageThreshold,
                this.totalThreshold,
                this.options);
        }

        public Kwargs stageThreshold(long stageThreshold) {
            return new Kwargs(
                this.transformationContext,
                this.callSite,
                stageThreshold,
                this.totalThreshold,
                this.options);
        }

        public Kwargs totalThreshold(long totalThreshold) {
            return new Kwargs(
                this.transformationContext,
                this.callSite,
                this.stageThreshold,
                totalThreshold,
                this.options);
        }

        public Kwargs options(JsonOptions options) {
            return new Kwargs(
                this.transformationContext,
                this.callSite,
                this.stageThreshold,
                this.totalThreshold,
                options);
        }

    }

}
