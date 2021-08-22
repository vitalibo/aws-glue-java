package com.github.vitalibo.glue.api.java;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.DynamicFrame;
import com.amazonaws.services.glue.PartitioningStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JavaDataSourceTest {

    @Mock
    private DataSource mockDataSource;
    @Mock
    private DynamicFrame mockDynamicFrame;
    @Mock
    private PartitioningStrategy mockPartitioningStrategy;
    @Mock
    private Dataset<Row> mockDataFrame;

    private JavaDataSource source;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        source = new JavaDataSource(mockDataSource);
    }

    @Test
    public void testGetDynamicFrame() {
        Mockito.when(mockDataSource.getDynamicFrame()).thenReturn(mockDynamicFrame);

        JavaDynamicFrame actual = source.getDynamicFrame();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDynamicFrame);
        Mockito.verify(mockDataSource).getDynamicFrame();
    }

    @Test
    public void testGetDynamicFramePartitioningStrategy() {
        Mockito.when(mockDataSource.getDynamicFrame(Mockito.any(PartitioningStrategy.class)))
            .thenReturn(mockDynamicFrame);

        JavaDynamicFrame actual = source.getDynamicFrame(mockPartitioningStrategy);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDynamicFrame);
        Mockito.verify(mockDataSource).getDynamicFrame(mockPartitioningStrategy);
    }

    @Test
    public void testGetDynamicFrameMinPartitions() {
        Mockito.when(mockDataSource.getDynamicFrame(Mockito.anyInt(), Mockito.anyInt()))
            .thenReturn(mockDynamicFrame);

        JavaDynamicFrame actual = source.getDynamicFrame(5, 10);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.delegate, mockDynamicFrame);
        Mockito.verify(mockDataSource).getDynamicFrame(5, 10);
    }

    @Test
    public void testGetDataFrame() {
        Mockito.when(mockDataSource.getDataFrame()).thenReturn(mockDataFrame);

        Dataset<Row> actual = source.getDataFrame();

        Assert.assertEquals(actual, mockDataFrame);
        Mockito.verify(mockDataSource).getDataFrame();
    }

}
