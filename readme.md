# AWS Glue ETL Java

Simple PoC that demonstrate usage Java in AWS Glue ETL pipelines.

### Examples

You can run these sample job scripts on any of AWS Glue ETL
jobs, [container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/),
or [local environment](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).

- [Clean and Process](https://github.com/aws-samples/aws-glue-samples/blob/master/examples/data_cleaning_and_lambda.md)

  This sample ETL script shows you how to take advantage of both Spark and AWS Glue features to clean and transform data
  for efficient analysis. A Java version you can find in this file [DataCleaningJob.java](/src/main/java/com/github/vitalibo/glue/job/DataCleaningJob.java)

- [Spark API](/src/main/java/com/github/vitalibo/glue/job/IncomeAvgJob.java)

  This sample ETL script show usage DataFrame and Dataset on AWS Glue.

## Programming AWS Glue ETL Scripts in Java

The following sections describe how to use the AWS Glue Java library and the AWS Glue API in ETL scripts, and provide
reference documentation for the library.

#### JavaDataSink

JavaDataSink encapsulates a destination and a format that a JavaDynamicFrame can be written to.

`com.github.vitalibo.glue.api.java.JavaDataSink`

#### JavaDataSource

JavaDataSource encapsulates a source and format that a JavaDynamicFrame can be produced from.

`com.github.vitalibo.glue.api.java.JavaDataSource`

#### JavaDynamicFrame

A JavaDynamicFrame is a distributed collection of self-describing DynamicRecord objects.

`com.github.vitalibo.glue.api.java.JavaDynamicFrame`

#### JavaGlueContext

JavaGlueContext is the entry point for reading and writing a JavaDynamicFrame.

`com.github.vitalibo.glue.api.java.JavaGlueContext`
