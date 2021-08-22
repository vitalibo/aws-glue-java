package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSink;
import com.amazonaws.services.glue.errors.CallSite;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JavaDataSink {

    public final DataSink delegate;

    public void writeDynamicFrame(JavaDynamicFrame frame) {
        writeDynamicFrame(frame, new CallSite("Not provided", ""));
    }

    public void writeDynamicFrame(JavaDynamicFrame frame, CallSite callSite) {
        delegate.writeDynamicFrame(frame.delegate, callSite);
    }

    public JavaDynamicFrame errorsAsDynamicFrame() {
        return new JavaDynamicFrame(delegate.errorsAsDynamicFrame());
    }

}
