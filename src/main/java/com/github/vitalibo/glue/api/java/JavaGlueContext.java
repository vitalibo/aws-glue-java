package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.JsonOptions;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;

@SuppressWarnings("PMD.ExcessiveParameterList")
@RequiredArgsConstructor
public class JavaGlueContext {

    public final GlueContext delegate;

    public JavaDataSink getCatalogSink(String database, String tableName) {
        return getCatalogSink(database, tableName, JavaGlueContext.kwargs());
    }

    public JavaDataSink getCatalogSink(String database, String tableName, Kwargs kwargs) {
        return new JavaDataSink(
            delegate.getCatalogSink(
                database,
                tableName,
                kwargs.redshiftTmpDir,
                kwargs.transformationContext,
                kwargs.options,
                kwargs.catalogId));
    }

    public JavaDataSource getCatalogSource(String database, String tableName) {
        return getCatalogSource(database, tableName, " ", JavaGlueContext.kwargs());
    }

    public JavaDataSource getCatalogSource(String database, String tableName, String pushDownPredicate) {
        return getCatalogSource(database, tableName, pushDownPredicate, JavaGlueContext.kwargs());
    }

    public JavaDataSource getCatalogSource(String database, String tableName, String pushDownPredicate, Kwargs kwargs) {
        return new JavaDataSource(
            delegate.getCatalogSource(
                database,
                tableName,
                kwargs.redshiftTmpDir,
                kwargs.transformationContext,
                pushDownPredicate,
                kwargs.options,
                kwargs.catalogId));
    }

    public JavaDataSink getJDBCSink(String catalogConnection) {
        return getJDBCSink(catalogConnection, JavaGlueContext.kwargs());
    }

    public JavaDataSink getJDBCSink(String catalogConnection, Kwargs kwargs) {
        return new JavaDataSink(
            delegate.getJDBCSink(
                catalogConnection,
                kwargs.options,
                kwargs.redshiftTmpDir,
                kwargs.transformationContext,
                kwargs.catalogId));
    }

    public JavaDataSink getSink(String connectionType, JsonOptions connectionOptions) {
        return getSink(connectionType, connectionOptions, JavaGlueContext.kwargs());
    }

    public JavaDataSink getSink(String connectionType, JsonOptions connectionOptions, Kwargs kwargs) {
        return new JavaDataSink(
            delegate.getSink(
                connectionType,
                connectionOptions,
                kwargs.transformationContext));
    }

    public JavaDataSink getSink(String connectionType, JsonOptions connectionOptions, String format, JsonOptions formatOptions) {
        return getSink(connectionType, connectionOptions, format, formatOptions, JavaGlueContext.kwargs());
    }

    public JavaDataSink getSink(String connectionType, JsonOptions connectionOptions, String format, JsonOptions formatOptions, Kwargs kwargs) {
        return new JavaDataSink(
            delegate.getSinkWithFormat(
                connectionType,
                connectionOptions,
                kwargs.transformationContext,
                format,
                formatOptions));
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions) {
        return getSource(connectionType, connectionOptions, " ");
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions, Kwargs kwargs) {
        return getSource(connectionType, connectionOptions, " ", kwargs);
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions, String pushDownPredicate) {
        return getSource(connectionType, connectionOptions, pushDownPredicate, JavaGlueContext.kwargs());
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions, String pushDownPredicate, Kwargs kwargs) {
        return new JavaDataSource(
            delegate.getSource(
                connectionType,
                connectionOptions,
                kwargs.transformationContext,
                pushDownPredicate));
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions, String format, JsonOptions formatOptions) {
        return getSource(connectionType, connectionOptions, format, formatOptions, JavaGlueContext.kwargs());
    }

    public JavaDataSource getSource(String connectionType, JsonOptions connectionOptions, String format, JsonOptions formatOptions, Kwargs kwargs) {
        return new JavaDataSource(
            delegate.getSourceWithFormat(
                connectionType,
                connectionOptions,
                kwargs.transformationContext,
                format,
                formatOptions));
    }

    public SparkSession getSparkSession() {
        return delegate.getSparkSession();
    }

    public static Kwargs kwargs() {
        return new Kwargs("", "", JsonOptions.empty(), null);
    }

    @SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
    @AllArgsConstructor
    public static class Kwargs {

        String redshiftTmpDir;
        String transformationContext;
        JsonOptions options;
        String catalogId;

        public Kwargs redshiftTmpDir(String redshiftTmpDir) {
            return new Kwargs(
                redshiftTmpDir,
                this.transformationContext,
                this.options,
                this.catalogId);
        }

        public Kwargs transformationContext(String transformationContext) {
            return new Kwargs(
                this.redshiftTmpDir,
                transformationContext,
                this.options,
                this.catalogId);
        }

        public Kwargs options(JsonOptions options) {
            return new Kwargs(
                this.redshiftTmpDir,
                this.transformationContext,
                options,
                this.catalogId);
        }

        public Kwargs catalogId(String catalogId) {
            return new Kwargs(
                this.redshiftTmpDir,
                this.transformationContext,
                this.options,
                catalogId);
        }

    }

}
