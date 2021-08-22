package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.*;
import com.amazonaws.services.glue.errors.CallSite;
import com.amazonaws.services.glue.schema.Schema;
import com.amazonaws.services.glue.util.JsonOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.*;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.github.vitalibo.glue.util.ScalaConverters.seq;
import static com.github.vitalibo.glue.util.ScalaConverters.tuple;

public class JavaDynamicFrameTest {

    @Mock
    private DynamicFrame mockDynamicFrame;
    @Mock
    private DynamicFrame mockDynamicFrame2;
    @Mock
    private DynamicFrame mockNewDynamicFrame;
    @Mock
    private DynamicFrame mockNewDynamicFrame2;
    @Mock
    private DynamicRecord mockDynamicRecord;
    @Mock
    private Schema mockSchema;
    @Mock
    private ChoiceOption mockChoiceOption;
    @Mock
    private Dataset<Row> mockDataFrame;
    @Mock
    private ResolveSpec mockResolveSpec;
    @Mock
    private GlueContext mockGlueContext;
    @Captor
    private ArgumentCaptor<Function1<DynamicRecord, Object>> captorPredicate;
    @Captor
    private ArgumentCaptor<Function1<DynamicRecord, DynamicRecord>> captorFunction1;
    @Captor
    private ArgumentCaptor<Function0<Schema>> captorSupplier;

    private JavaDynamicFrame frame;
    private JavaDynamicFrame frame2;
    private CallSite callSite;
    private JsonOptions options;
    private JavaDynamicFrame.Kwargs kwargs;
    private MockedStatic<DynamicFrame> mockStaticDynamicFrame;
    private JavaGlueContext context;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        frame = new JavaDynamicFrame(mockDynamicFrame);
        frame2 = new JavaDynamicFrame(mockDynamicFrame2);
        callSite = new CallSite("foo", "bar");
        options = new JsonOptions("{}");
        kwargs = JavaDynamicFrame.kwargs()
            .transformationContext("ctx")
            .callSite(callSite)
            .stageThreshold(12)
            .totalThreshold(23)
            .options(options);
        mockStaticDynamicFrame = Mockito.mockStatic(DynamicFrame.class);
        context = new JavaGlueContext(mockGlueContext);
    }

    @AfterMethod
    public void cleanUp() {
        mockStaticDynamicFrame.close();
    }

    @Test
    public void testErrorsCount() {
        Mockito.when(mockDynamicFrame.errorsCount()).thenReturn(123L);

        long actual = frame.errorsCount();

        Assert.assertEquals(actual, 123L);
        Mockito.verify(mockDynamicFrame).errorsCount();
    }

    @Test
    public void testApplyMapping() {
        Mockito.when(mockDynamicFrame.applyMapping(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.applyMapping(
            tuple("foo0", "foo1", "foo2", "foo3"),
            tuple("bar0", "bar1", "bar2", "bar3"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).applyMapping(seq(tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")), true, "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testApplyMappingCaseSensitive() {
        Mockito.when(mockDynamicFrame.applyMapping(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.applyMapping(new Tuple4[]{tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")}, false);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).applyMapping(seq(tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")), false, "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testApplyMappingKwargs() {
        Mockito.when(mockDynamicFrame.applyMapping(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.applyMapping(new Tuple4[]{tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).applyMapping(seq(tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")), true, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testApplyMappingCaseSensitiveKwargs() {
        Mockito.when(mockDynamicFrame.applyMapping(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.applyMapping(new Tuple4[]{tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")}, false, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).applyMapping(seq(tuple("foo0", "foo1", "foo2", "foo3"), tuple("bar0", "bar1", "bar2", "bar3")), false, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testAssertErrorThreshold() {
        frame.assertErrorThreshold();

        Mockito.verify(mockDynamicFrame).assertErrorThreshold();
    }

    @Test
    public void testCount() {
        frame.count();

        Mockito.verify(mockDynamicFrame).count();
    }

    @Test
    public void testDropField() {
        Mockito.when(mockDynamicFrame.dropField(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropField("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropField("foo", "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testDropFieldKwargs() {
        Mockito.when(mockDynamicFrame.dropField(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropField("foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropField("foo", "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testDropFields() {
        Mockito.when(mockDynamicFrame.dropFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropFields("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropFields(seq("foo", "bar"), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testDropFieldsKwargs() {
        Mockito.when(mockDynamicFrame.dropFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropFields(new String[]{"foo", "bar"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropFields(seq("foo", "bar"), "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testDropNulls() {
        Mockito.when(mockDynamicFrame.dropNulls(Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropNulls();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropNulls("", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testDropNullsKwargs() {
        Mockito.when(mockDynamicFrame.dropNulls(Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.dropNulls(kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).dropNulls("ctx", callSite, 12L, 23L);
    }

    @Test
    public void testErrorsAsDynamicFrame() {
        Mockito.when(mockDynamicFrame.errorsAsDynamicFrame())
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.errorsAsDynamicFrame();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).errorsAsDynamicFrame();
    }

    @Test
    public void testFilter() {
        Mockito.when(mockDynamicFrame.filter(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.filter(Objects::isNull);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).filter(captorPredicate.capture(), Mockito.eq(""), Mockito.eq(""), Mockito.eq(new CallSite("Not provided", "")), Mockito.eq(0L), Mockito.eq(0L));
        Function1<DynamicRecord, Object> predicate = captorPredicate.getValue();
        Assert.assertFalse((boolean) predicate.apply(mockDynamicRecord));
        Assert.assertTrue((boolean) predicate.apply(null));
    }

    @Test
    public void testFilterErrorMsg() {
        Mockito.when(mockDynamicFrame.filter(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.filter(Objects::isNull, "foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).filter(captorPredicate.capture(), Mockito.eq("foo"), Mockito.eq(""), Mockito.eq(new CallSite("Not provided", "")), Mockito.eq(0L), Mockito.eq(0L));
        Function1<DynamicRecord, Object> predicate = captorPredicate.getValue();
        Assert.assertFalse((boolean) predicate.apply(mockDynamicRecord));
        Assert.assertTrue((boolean) predicate.apply(null));
    }

    @Test
    public void testFilterKwargs() {
        Mockito.when(mockDynamicFrame.filter(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.filter(Objects::isNull, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).filter(captorPredicate.capture(), Mockito.eq(""), Mockito.eq("ctx"), Mockito.eq(callSite), Mockito.eq(12L), Mockito.eq(23L));
        Function1<DynamicRecord, Object> predicate = captorPredicate.getValue();
        Assert.assertFalse((boolean) predicate.apply(mockDynamicRecord));
        Assert.assertTrue((boolean) predicate.apply(null));
    }

    @Test
    public void testFilterErrorMsgKwargs() {
        Mockito.when(mockDynamicFrame.filter(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.filter(Objects::isNull, "foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).filter(captorPredicate.capture(), Mockito.eq("foo"), Mockito.eq("ctx"), Mockito.eq(callSite), Mockito.eq(12L), Mockito.eq(23L));
        Function1<DynamicRecord, Object> predicate = captorPredicate.getValue();
        Assert.assertFalse((boolean) predicate.apply(mockDynamicRecord));
        Assert.assertTrue((boolean) predicate.apply(null));
    }

    @Test
    public void testGetName() {
        Mockito.when(mockDynamicFrame.getName()).thenReturn("foo");

        String actual = frame.getName();

        Assert.assertEquals(actual, "foo");
    }

    @Test
    public void testGetNumPartitions() {
        Mockito.when(mockDynamicFrame.getNumPartitions()).thenReturn(123);

        int actual = frame.getNumPartitions();

        Assert.assertEquals(actual, 123);
    }

    @Test
    public void testGetSchemaIfComputed() {
        Mockito.when(mockDynamicFrame.getSchemaIfComputed()).thenReturn(Option.apply(mockSchema));

        Optional<Schema> actual = frame.getSchemaIfComputed();

        Assert.assertTrue(actual.isPresent());
        Assert.assertEquals(actual.get(), mockSchema);
    }


    @Test
    public void testGetSchemaIfComputedEmpty() {
        Mockito.when(mockDynamicFrame.getSchemaIfComputed()).thenReturn(Option.empty());

        Optional<Schema> actual = frame.getSchemaIfComputed();

        Assert.assertFalse(actual.isPresent());
    }

    @Test
    public void testIsSchemaComputed() {
        Mockito.when(mockDynamicFrame.isSchemaComputed()).thenReturn(true);

        boolean actual = frame.isSchemaComputed();

        Assert.assertTrue(actual);
    }

    @Test
    public void testJoin() {
        Mockito.when(mockDynamicFrame.join(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.join(frame2, "foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).join(seq("foo", "bar"), seq("foo", "bar"), mockDynamicFrame2, "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testJoinKwargs() {
        Mockito.when(mockDynamicFrame.join(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.join(frame2, new String[]{"foo", "bar"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).join(seq("foo", "bar"), seq("foo", "bar"), mockDynamicFrame2, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testJoinDiffKey() {
        Mockito.when(mockDynamicFrame.join(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.join(frame2, new String[]{"foo", "bar"}, new String[]{"baz", "taz"});

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).join(seq("foo", "bar"), seq("baz", "taz"), mockDynamicFrame2, "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testJoinDiffKeyKwargs() {
        Mockito.when(mockDynamicFrame.join(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.join(frame2, new String[]{"foo", "bar"}, new String[]{"baz", "taz"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).join(seq("foo", "bar"), seq("baz", "taz"), mockDynamicFrame2, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testMap() {
        Mockito.when(mockDynamicFrame.map(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.map(o -> o);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).map(captorFunction1.capture(), Mockito.eq(""), Mockito.eq(""), Mockito.eq(new CallSite("Not provided", "")), Mockito.eq(0L), Mockito.eq(0L));
        Function1<DynamicRecord, DynamicRecord> function = captorFunction1.getValue();
        Assert.assertEquals(function.apply(mockDynamicRecord), mockDynamicRecord);
    }

    @Test
    public void testMapErrorMsg() {
        Mockito.when(mockDynamicFrame.map(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.map(o -> o, "foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).map(captorFunction1.capture(), Mockito.eq("foo"), Mockito.eq(""), Mockito.eq(new CallSite("Not provided", "")), Mockito.eq(0L), Mockito.eq(0L));
        Function1<DynamicRecord, DynamicRecord> function = captorFunction1.getValue();
        Assert.assertEquals(function.apply(mockDynamicRecord), mockDynamicRecord);
    }

    @Test
    public void testMapKwargs() {
        Mockito.when(mockDynamicFrame.map(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.map(o -> o, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).map(captorFunction1.capture(), Mockito.eq(""), Mockito.eq("ctx"), Mockito.eq(callSite), Mockito.eq(12L), Mockito.eq(23L));
        Function1<DynamicRecord, DynamicRecord> function = captorFunction1.getValue();
        Assert.assertEquals(function.apply(mockDynamicRecord), mockDynamicRecord);
    }

    @Test
    public void testMapErrorMsgKwargs() {
        Mockito.when(mockDynamicFrame.map(Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.map(o -> o, "foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).map(captorFunction1.capture(), Mockito.eq("foo"), Mockito.eq("ctx"), Mockito.eq(callSite), Mockito.eq(12L), Mockito.eq(23L));
        Function1<DynamicRecord, DynamicRecord> function = captorFunction1.getValue();
        Assert.assertEquals(function.apply(mockDynamicRecord), mockDynamicRecord);
    }

    @Test
    public void testMergeDynamicFrames() {
        Mockito.when(mockDynamicFrame.mergeDynamicFrames(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.mergeDynamicFrames(frame2, "foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).mergeDynamicFrames(mockDynamicFrame2, seq("foo", "bar"), "", JsonOptions.empty(), new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testMergeDynamicFramesKwargs() {
        Mockito.when(mockDynamicFrame.mergeDynamicFrames(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.mergeDynamicFrames(frame2, new String[]{"foo", "bar"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).mergeDynamicFrames(mockDynamicFrame2, seq("foo", "bar"), "ctx", options, callSite, 12L, 23L);
    }

    @Test
    public void testPrintSchema() {
        frame.printSchema();

        Mockito.verify(mockDynamicFrame).printSchema();
    }

    @Test
    public void testRecomputeSchema() {
        Mockito.when(mockDynamicFrame.recomputeSchema()).thenReturn(mockSchema);

        Schema actual = frame.recomputeSchema();

        Assert.assertEquals(actual, mockSchema);
        Mockito.verify(mockDynamicFrame).recomputeSchema();
    }

    @Test
    public void testRelationalize() {
        Mockito.when(mockDynamicFrame.relationalize(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.relationalize("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).relationalize("foo", "bar", JsonOptions.empty(), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testRelationalizeKwargs() {
        Mockito.when(mockDynamicFrame.relationalize(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.relationalize("foo", "bar", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).relationalize("foo", "bar", options, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testRenameField() {
        Mockito.when(mockDynamicFrame.renameField(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.renameField("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).renameField("foo", "bar", "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testRenameFieldKwargs() {
        Mockito.when(mockDynamicFrame.renameField(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.renameField("foo", "bar", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).renameField("foo", "bar", "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testRepartition() {
        Mockito.when(mockDynamicFrame.repartition(Mockito.anyInt(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.repartition(10);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).repartition(10, "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testRepartitionKwargs() {
        Mockito.when(mockDynamicFrame.repartition(Mockito.anyInt(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.repartition(10, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).repartition(10, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testResolveChoice() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(tuple("foo", "bar"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.empty(), Option.empty(), Option.empty(), "", new CallSite("Not provided", ""), 0L, 0L, Option.empty());
    }

    @Test
    public void testResolveChoiceKwarg() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(new Tuple2[]{tuple("foo", "bar")}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.empty(), Option.empty(), Option.empty(), "ctx", callSite, 12L, 23L, Option.empty());
    }

    @Test
    public void testResolveChoiceChoiceOption() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(new Tuple2[]{tuple("foo", "bar")}, mockChoiceOption);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.apply(mockChoiceOption), Option.empty(), Option.empty(), "", new CallSite("Not provided", ""), 0L, 0L, Option.empty());
    }

    @Test
    public void testResolveChoiceChoiceOptionKwarg() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(new Tuple2[]{tuple("foo", "bar")}, mockChoiceOption, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.apply(mockChoiceOption), Option.empty(), Option.empty(), "ctx", callSite, 12L, 23L, Option.empty());
    }

    @Test
    public void testResolveChoiceDB() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(new Tuple2[]{tuple("foo", "bar")}, mockChoiceOption, "db", "tb", "123");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.apply(mockChoiceOption), Option.apply("db"), Option.apply("tb"), "", new CallSite("Not provided", ""), 0L, 0L, Option.apply("123"));
    }

    @Test
    public void testResolveChoiceDBKwarg() {
        Mockito.when(mockDynamicFrame.resolveChoice(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.resolveChoice(new Tuple2[]{tuple("foo", "bar")}, mockChoiceOption, "db", "tb", "123", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).resolveChoice(seq(tuple("foo", "bar")), Option.apply(mockChoiceOption), Option.apply("db"), Option.apply("tb"), "ctx", callSite, 12L, 23L, Option.apply("123"));
    }

    @Test
    public void testSchema() {
        Mockito.when(mockDynamicFrame.schema()).thenReturn(mockSchema);

        Schema actual = frame.schema();

        Assert.assertEquals(actual, mockSchema);
        Mockito.verify(mockDynamicFrame).schema();
    }

    @Test
    public void testSelectField() {
        Mockito.when(mockDynamicFrame.selectField(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.selectField("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).selectField("foo", "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testSelectFieldKwargs() {
        Mockito.when(mockDynamicFrame.selectField(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.selectField("foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).selectField("foo", "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testSelectFields() {
        Mockito.when(mockDynamicFrame.selectFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.selectFields("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).selectFields(seq("foo", "bar"), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testSelectFieldsKwargs() {
        Mockito.when(mockDynamicFrame.selectFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.selectFields(new String[]{"foo", "bar"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).selectFields(seq("foo", "bar"), "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testShowDefault() {
        frame.show();

        Mockito.verify(mockDynamicFrame).show(20);
    }

    @Test
    public void testShow() {
        frame.show(10);

        Mockito.verify(mockDynamicFrame).show(10);
    }

    @Test
    public void testSpigot() {
        Mockito.when(mockDynamicFrame.spigot(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.spigot("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).spigot("foo", JsonOptions.empty(), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testSpigotKwargs() {
        Mockito.when(mockDynamicFrame.spigot(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.spigot("foo", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).spigot("foo", options, "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testSplitFields() {
        Mockito.when(mockDynamicFrame.splitFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.splitFields("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 2);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).splitFields(seq("foo", "bar"), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testSplitFieldsKwargs() {
        Mockito.when(mockDynamicFrame.splitFields(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.splitFields(new String[]{"foo", "bar"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 2);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).splitFields(seq("foo", "bar"), "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testSplitRows() {
        Mockito.when(mockDynamicFrame.splitRows(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.splitRows(new String[]{"foo", "bar"}, new String[]{"foo1", "bar1"}, new String[]{"foo2", "bar2"});

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 2);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).splitRows(seq("foo", "bar"), seq("foo1", "bar1"), seq("foo2", "bar2"), "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testSplitRowsKwargs() {
        Mockito.when(mockDynamicFrame.splitRows(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(seq(mockNewDynamicFrame, mockNewDynamicFrame2));

        List<JavaDynamicFrame> actual = frame.splitRows(new String[]{"foo", "bar"}, new String[]{"foo1", "bar1"}, new String[]{"foo2", "bar2"}, kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 2);
        Assert.assertEquals(actual.get(0).delegate, mockNewDynamicFrame);
        Assert.assertEquals(actual.get(1).delegate, mockNewDynamicFrame2);
        Mockito.verify(mockDynamicFrame).splitRows(seq("foo", "bar"), seq("foo1", "bar1"), seq("foo2", "bar2"), "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testStageErrorsCount() {
        Mockito.when(mockDynamicFrame.stageErrorsCount()).thenReturn(123L);

        long actual = frame.stageErrorsCount();

        Assert.assertEquals(actual, 123);
        Mockito.verify(mockDynamicFrame).stageErrorsCount();
    }

    @Test
    public void testToDF() {
        Mockito.when(mockDynamicFrame.toDF(Mockito.any())).thenReturn(mockDataFrame);

        Dataset<Row> actual = frame.toDF();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockDataFrame);
        Mockito.verify(mockDynamicFrame).toDF(seq());
    }

    @Test
    public void testToDFResolveSpec() {
        Mockito.when(mockDynamicFrame.toDF(Mockito.any())).thenReturn(mockDataFrame);

        Dataset<Row> actual = frame.toDF(mockResolveSpec);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockDataFrame);
        Mockito.verify(mockDynamicFrame).toDF(seq(mockResolveSpec));
    }

    @Test
    public void testUnbox() {
        Mockito.when(mockDynamicFrame.unbox(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unbox("foo", "bar");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unbox("foo", "bar", "{}", "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testUnboxOptionString() {
        Mockito.when(mockDynamicFrame.unbox(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unbox("foo", "bar", "{\"baz\": 124}");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unbox("foo", "bar", "{\"baz\": 124}", "", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testUnboxKwargs() {
        Mockito.when(mockDynamicFrame.unbox(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unbox("foo", "bar", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unbox("foo", "bar", "{}", "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testUnboxOptionStringKwargs() {
        Mockito.when(mockDynamicFrame.unbox(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unbox("foo", "bar", "{\"baz\": 124}", kwargs);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unbox("foo", "bar", "{\"baz\": 124}", "ctx", callSite, 12L, 23L);
    }

    @Test
    public void testUnnest() {
        Mockito.when(mockDynamicFrame.unnest(Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unnest();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unnest("", new CallSite("Not provided", ""), 0L, 0L);
    }

    @Test
    public void testUnnestKwargs() {
        Mockito.when(mockDynamicFrame.unnest(Mockito.anyString(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.unnest(kwargs);
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).unnest("ctx", callSite, 12L, 23L);
    }

    @Test
    public void testWithFrameSchema() {
        Mockito.when(mockDynamicFrame.withFrameSchema(Mockito.any())).thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.withFrameSchema(() -> mockSchema);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).withFrameSchema(captorSupplier.capture());
        Function0<Schema> supplier = captorSupplier.getValue();
        Assert.assertEquals(supplier.apply(), mockSchema);
    }

    @Test
    public void testWithName() {
        Mockito.when(mockDynamicFrame.withName(Mockito.anyString())).thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.withName("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).withName("foo");
    }

    @Test
    public void testWithTransformationContext() {
        Mockito.when(mockDynamicFrame.withTransformationContext(Mockito.anyString())).thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = frame.withTransformationContext("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        Mockito.verify(mockDynamicFrame).withTransformationContext("foo");
    }

    @Test
    public void testFrom() {
        mockStaticDynamicFrame.when(() -> DynamicFrame.apply(Mockito.any(), Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = JavaDynamicFrame.from(mockDataFrame, context);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        mockStaticDynamicFrame.verify(Mockito.times(1), () -> DynamicFrame.apply(mockDataFrame, mockGlueContext));
    }

    @Test
    public void testEmptyDynamicFrame() {
        mockStaticDynamicFrame.when(() -> DynamicFrame.emptyDynamicFrame(Mockito.any()))
            .thenReturn(mockNewDynamicFrame);

        JavaDynamicFrame actual = JavaDynamicFrame.emptyDynamicFrame(context);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockNewDynamicFrame);
        mockStaticDynamicFrame.verify(Mockito.times(1), () -> DynamicFrame.emptyDynamicFrame(mockGlueContext));
    }

    @Test
    public void testKwargs() {
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "");
        Assert.assertEquals(actual.callSite, new CallSite("Not provided", ""));
        Assert.assertEquals(actual.stageThreshold, 0L);
        Assert.assertEquals(actual.totalThreshold, 0L);
        Assert.assertEquals(actual.options, JsonOptions.empty());
    }

    @Test
    public void testKwargsTransformationContext() {
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs()
            .transformationContext("foo");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "foo");
        Assert.assertEquals(actual.callSite, new CallSite("Not provided", ""));
        Assert.assertEquals(actual.stageThreshold, 0L);
        Assert.assertEquals(actual.totalThreshold, 0L);
        Assert.assertEquals(actual.options, JsonOptions.empty());
    }

    @Test
    public void testKwargsCallSite() {
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs()
            .callSite(new CallSite("foo", "bar"));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "");
        Assert.assertEquals(actual.callSite, new CallSite("foo", "bar"));
        Assert.assertEquals(actual.stageThreshold, 0L);
        Assert.assertEquals(actual.totalThreshold, 0L);
        Assert.assertEquals(actual.options, JsonOptions.empty());
    }

    @Test
    public void testKwargsStageThreshold() {
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs()
            .stageThreshold(123L);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "");
        Assert.assertEquals(actual.callSite, new CallSite("Not provided", ""));
        Assert.assertEquals(actual.stageThreshold, 123L);
        Assert.assertEquals(actual.totalThreshold, 0L);
        Assert.assertEquals(actual.options, JsonOptions.empty());
    }

    @Test
    public void testKwargsTotalThreshold() {
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs()
            .totalThreshold(123L);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "");
        Assert.assertEquals(actual.callSite, new CallSite("Not provided", ""));
        Assert.assertEquals(actual.stageThreshold, 0L);
        Assert.assertEquals(actual.totalThreshold, 123L);
        Assert.assertEquals(actual.options, JsonOptions.empty());
    }

    @Test
    public void testKwargsOptions() {
        JsonOptions options = new JsonOptions("{\"foo\":\"bar\"}");
        JavaDynamicFrame.Kwargs actual = JavaDynamicFrame.kwargs()
            .options(options);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.transformationContext, "");
        Assert.assertEquals(actual.callSite, new CallSite("Not provided", ""));
        Assert.assertEquals(actual.stageThreshold, 0L);
        Assert.assertEquals(actual.totalThreshold, 0L);
        Assert.assertEquals(actual.options, options);
    }

}
