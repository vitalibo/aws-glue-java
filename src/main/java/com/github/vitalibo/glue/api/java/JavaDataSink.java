package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSink;
import com.amazonaws.services.glue.errors.CallSite;
import com.amazonaws.services.glue.output.OutputError;
import lombok.RequiredArgsConstructor;
import org.apache.spark.Accumulable;
import scala.collection.immutable.List;

@RequiredArgsConstructor
public class JavaDataSink {

    public final DataSink delegate;

    public void writeDynamicFrame(JavaDynamicFrame frame) {
        writeDynamicFrame(frame, new CallSite("Not provided", ""));
    }

    public void writeDynamicFrame(JavaDynamicFrame frame, CallSite callSite) {
        delegate.writeDynamicFrame(frame.delegate, callSite);
    }

    public boolean supportsFormat(String format) {
        return delegate.supportsFormat(format);
    }

    public void setAccumulableSize(int size) {
        delegate.setAccumulableSize(size);
    }

    public Accumulable<List<OutputError>, OutputError> getOutputErrorRecordsAccumulable() {
        return delegate.getOutputErrorRecordsAccumulable();
    }

    public JavaDynamicFrame errorsAsDynamicFrame() {
        return new JavaDynamicFrame(delegate.errorsAsDynamicFrame());
    }

}
