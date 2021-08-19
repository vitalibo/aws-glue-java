package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.PartitioningStrategy;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@RequiredArgsConstructor
public class JavaDataSource {

    public final DataSource delegate;

    public JavaDynamicFrame getDynamicFrame(PartitioningStrategy partitioner) {
        return new JavaDynamicFrame(delegate.getDynamicFrame(partitioner));
    }

    public JavaDynamicFrame getDynamicFrame() {
        return new JavaDynamicFrame(delegate.getDynamicFrame());
    }

    public JavaDynamicFrame getDynamicFrame(int minPartitions, int targetPartitions) {
        return new JavaDynamicFrame(delegate.getDynamicFrame(minPartitions, targetPartitions));
    }

    public boolean supportsFormat(String format) {
        return delegate.supportsFormat(format);
    }

    public Dataset<Row> getDataFrame() {
        return delegate.getDataFrame();
    }

}
