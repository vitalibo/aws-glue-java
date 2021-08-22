package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSink;
import com.amazonaws.services.glue.DynamicFrame;
import com.amazonaws.services.glue.errors.CallSite;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JavaDataSinkTest {

    @Mock
    private DataSink mockDataSink;
    @Mock
    private DynamicFrame mockDynamicFrame;
    @Mock
    private CallSite mockCallSite;
    @Captor
    private ArgumentCaptor<CallSite> captorCallSite;

    private JavaDataSink sink;
    private JavaDynamicFrame frame;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        sink = new JavaDataSink(mockDataSink);
        frame = new JavaDynamicFrame(mockDynamicFrame);
    }

    @Test
    public void testWriteDynamicFrame() {
        sink.writeDynamicFrame(frame);

        Mockito.verify(mockDataSink).writeDynamicFrame(Mockito.eq(mockDynamicFrame), captorCallSite.capture());
        CallSite callSite = captorCallSite.getValue();
        Assert.assertEquals(callSite.site(), "Not provided");
        Assert.assertEquals(callSite.info(), "");
    }

    @Test
    public void testWriteDynamicFrameCallSite() {
        sink.writeDynamicFrame(frame, mockCallSite);

        Mockito.verify(mockDataSink).writeDynamicFrame(mockDynamicFrame, mockCallSite);
    }

    @Test
    public void testErrorsAsDynamicFrame() {
        Mockito.when(mockDataSink.errorsAsDynamicFrame()).thenReturn(mockDynamicFrame);

        JavaDynamicFrame actual = sink.errorsAsDynamicFrame();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDynamicFrame);
        Mockito.verify(mockDataSink).errorsAsDynamicFrame();
    }

}
