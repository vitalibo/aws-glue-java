package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSink;
import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.JsonOptions;
import org.apache.spark.sql.SparkSession;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JavaGlueContextTest {

    @Mock
    private GlueContext mockGlueContext;
    @Mock
    private DataSink mockDataSink;
    @Mock
    private DataSource mockDataSource;
    @Mock
    private SparkSession mockSparkSession;

    private JavaGlueContext context;
    private JavaGlueContext.Kwargs kwargs;
    private JsonOptions options;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        context = new JavaGlueContext(mockGlueContext);
        options = new JsonOptions("{\"someKey\":\"someValue\"}");
        kwargs = JavaGlueContext.kwargs()
            .redshiftTmpDir("tmp_dir")
            .transformationContext("ctx")
            .options(options)
            .catalogId("123");
    }

    @Test
    public void testGetCatalogSink() {
        Mockito.when(mockGlueContext.getCatalogSink(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getCatalogSink("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getCatalogSink("foo", "bar", "", "", JsonOptions.empty(), null);
    }

    @Test
    public void testGetCatalogSinkKwargs() {
        Mockito.when(mockGlueContext.getCatalogSink(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getCatalogSink("foo", "bar", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getCatalogSink("foo", "bar", "tmp_dir", "ctx", options, "123");
    }

    @Test
    public void testGetCatalogSource() {
        Mockito.when(mockGlueContext.getCatalogSource(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getCatalogSource("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getCatalogSource("foo", "bar", "", "", " ", JsonOptions.empty(), null);
    }

    @Test
    public void testGetCatalogSourcePushDownPredicate() {
        Mockito.when(mockGlueContext.getCatalogSource(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getCatalogSource("foo", "bar", "foo > 0");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getCatalogSource("foo", "bar", "", "", "foo > 0", JsonOptions.empty(), null);
    }

    @Test
    public void testGetCatalogSourceKwargs() {
        Mockito.when(mockGlueContext.getCatalogSource(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getCatalogSource("foo", "bar", "foo > 0", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getCatalogSource("foo", "bar", "tmp_dir", "ctx", "foo > 0", options, "123");
    }

    @Test
    public void testGetJDBCSink() {
        Mockito.when(mockGlueContext.getJDBCSink(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getJDBCSink("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getJDBCSink("foo", JsonOptions.empty(), "", "", null);
    }

    @Test
    public void testGetJDBCSinkKwargs() {
        Mockito.when(mockGlueContext.getJDBCSink(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getJDBCSink("foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getJDBCSink("foo", options, "tmp_dir", "ctx", "123");
    }

    @Test
    public void testGetSink() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSink(Mockito.anyString(), Mockito.any(), Mockito.anyString()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getSink("s3", connectionOptions);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getSink("s3", connectionOptions, "");
    }

    @Test
    public void testGetSinkKwargs() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSink(Mockito.anyString(), Mockito.any(), Mockito.anyString()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getSink("s3", connectionOptions, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getSink("s3", connectionOptions, "ctx");
    }

    @Test
    public void testGetSinkWithFormat() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        JsonOptions formatOptions = new JsonOptions("{\"separator\": \",\"}");
        Mockito.when(mockGlueContext.getSinkWithFormat(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getSink("s3", connectionOptions, "csv", formatOptions);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getSinkWithFormat("s3", connectionOptions, "", "csv", formatOptions);
    }

    @Test
    public void testGetSinkWithFormatKwargs() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        JsonOptions formatOptions = new JsonOptions("{\"separator\": \",\"}");
        Mockito.when(mockGlueContext.getSinkWithFormat(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSink);

        JavaDataSink actual = context.getSink("s3", connectionOptions, "csv", formatOptions, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSink);
        Mockito.verify(mockGlueContext).getSinkWithFormat("s3", connectionOptions, "ctx", "csv", formatOptions);
    }

    @Test
    public void testGetSource() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSource(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSource("s3", connectionOptions, "", " ");
    }

    @Test
    public void testGetSourcePushDownPredicate() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSource(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions, "foo > 3");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSource("s3", connectionOptions, "", "foo > 3");
    }

    @Test
    public void testGetSourceKwargs() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSource(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSource("s3", connectionOptions, "ctx", " ");
    }

    @Test
    public void testGetSourcePushDownPredicateKwargs() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        Mockito.when(mockGlueContext.getSource(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions, "foo > 3", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSource("s3", connectionOptions, "ctx", "foo > 3");
    }

    @Test
    public void testGetSourceWithFormat() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        JsonOptions formatOptions = new JsonOptions("{\"separator\": \",\"}");
        Mockito.when(mockGlueContext.getSourceWithFormat(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions, "csv", formatOptions);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSourceWithFormat("s3", connectionOptions, "", "csv", formatOptions);
    }

    @Test
    public void testGetSourceWithFormatKwargs() {
        JsonOptions connectionOptions = new JsonOptions("{\"path\":\"s3://foo/bar\"}");
        JsonOptions formatOptions = new JsonOptions("{\"separator\": \",\"}");
        Mockito.when(mockGlueContext.getSourceWithFormat(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(mockDataSource);

        JavaDataSource actual = context.getSource("s3", connectionOptions, "csv", formatOptions, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDataSource);
        Mockito.verify(mockGlueContext).getSourceWithFormat("s3", connectionOptions, "ctx", "csv", formatOptions);
    }

    @Test
    public void testGetSparkSession() {
        Mockito.when(mockGlueContext.getSparkSession()).thenReturn(mockSparkSession);

        SparkSession actual = context.getSparkSession();

        Assert.assertEquals(actual, mockSparkSession);
        Mockito.verify(mockGlueContext).getSparkSession();
    }

    @Test
    public void testKwargs() {
        JavaGlueContext.Kwargs kwargs = JavaGlueContext.kwargs();

        Assert.assertEquals(kwargs.redshiftTmpDir, "");
        Assert.assertEquals(kwargs.transformationContext, "");
        Assert.assertEquals(kwargs.options, JsonOptions.empty());
        Assert.assertEquals(kwargs.catalogId, (String) null);
    }

    @Test
    public void testKwargsRedshiftTmpDir() {
        JavaGlueContext.Kwargs kwargs = JavaGlueContext.kwargs()
            .redshiftTmpDir("tmp_dir");

        Assert.assertEquals(kwargs.redshiftTmpDir, "tmp_dir");
        Assert.assertEquals(kwargs.transformationContext, "");
        Assert.assertEquals(kwargs.options, JsonOptions.empty());
        Assert.assertEquals(kwargs.catalogId, (String) null);
    }

    @Test
    public void testKwargsTransformationContext() {
        JavaGlueContext.Kwargs kwargs = JavaGlueContext.kwargs()
            .transformationContext("ctx");

        Assert.assertEquals(kwargs.redshiftTmpDir, "");
        Assert.assertEquals(kwargs.transformationContext, "ctx");
        Assert.assertEquals(kwargs.options, JsonOptions.empty());
        Assert.assertEquals(kwargs.catalogId, (String) null);
    }

    @Test
    public void testKwargsOption() {
        JsonOptions options = new JsonOptions("{\"foo\":\"bar\"}");
        JavaGlueContext.Kwargs kwargs = JavaGlueContext.kwargs()
            .options(options);

        Assert.assertEquals(kwargs.redshiftTmpDir, "");
        Assert.assertEquals(kwargs.transformationContext, "");
        Assert.assertEquals(kwargs.options, options);
        Assert.assertEquals(kwargs.catalogId, (String) null);
    }

    @Test
    public void testKwargsCatalogId() {
        JavaGlueContext.Kwargs kwargs = JavaGlueContext.kwargs()
            .catalogId("123");

        Assert.assertEquals(kwargs.redshiftTmpDir, "");
        Assert.assertEquals(kwargs.transformationContext, "");
        Assert.assertEquals(kwargs.options, JsonOptions.empty());
        Assert.assertEquals(kwargs.catalogId, "123");
    }

}
