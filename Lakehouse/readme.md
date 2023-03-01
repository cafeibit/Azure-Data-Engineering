# Implement Azure Databricks Lakehouse and build incremental workflows

We choose Azure Databricks to build the data engineering and analytics platform in the Azure cloud. 
Before we implement the Lakehouse platform we need to explore the external data from the ingested raw data by ADF in the Azure Data Lake, 
then we can use Auto Loader function to build incremental ETL pipelines as the multi-hop architecture - "Boronze", "Silver" and "Gold" data layer.

 - Build ETL pipelines using Apache Spark SQL and Python with Databricks in Lakehouse
   -- Create a cluster on Databricks that can run Apache Spark SQL and Python workloads.
   -- Explore the source data that you want to ingest and transform using interactive notebooks.
   -- Ingest raw data to Delta Lake, an open-source storage layer that provides reliability and performance for your data lake.
   -- Prepare raw data and write to Delta Lake using Apache Spark SQL or Python commands. You can also apply business logic and transformations as needed.
   -- Query the transformed data using Apache Spark SQL or Python. You can also use BI tools or machine learning frameworks to analyze the data.
   -- Create a Databricks job to run the pipeline on a schedule or on demand12.
Optionally, you can also use Delta Live Tables, a feature that simplifies and modernizes the approach to building ETL pipelines by allowing you to use SQL or Python to define declarative pipelines. Delta Live Tables automatically manages all the dependencies within the pipeline and handles schema evolution, error handling, monitoring etc.
 - Design and implement the incremental auto loader pipelines
 - Orchestrate and monitor the workflows by ADF and DLT
 - Test and build the report dashboard
 - Data govenance and more

## EDA for the raw data and prepare for Lakehouse 

Azure Databricks supports the directly querying files well for self-describing formats such as csv.  Then we will create tables using external data sources. While these tables will be stored in the Delta Lake format  be optimized for the Lakehouse.

<code>SELECT * FROM csv.`${DA.paths.sales_csv}`</code>

We can see from the above that:
1. The header row is being extracted as a table row
2. All columns are being loaded as a single column
3. The file is pipe-delimited (**`|`**)
4. The final column appears to contain nested data that is being truncated

### Registering Tables on External Data with Read Options

While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options. And there are many additional configurations we can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.

<strong><code>
CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
USING data_source<br/>
OPTIONS (key1 = val1, key2 = val2, ...)<br/>
LOCATION = path<br/>
</code></strong>

Running **`DESCRIBE EXTENDED`** on a table will show all of the metadata associated with the table definition.

## Incremental Data Ingestion with Auto Loader

Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data reduces redundant processing and helps enterprises reliably scale data pipelines.
The first step for any successful data lakehouse implementation is ingesting into a Delta Lake table from cloud storage. 
Historically, ingesting files from a data lake into a database has been a complicated process. Databricks Auto Loader provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage. 

Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage of the pipeline represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.

- **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, to name a few examples).
- **Silver** tables provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
- **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. The end outputs are actionable insights, dashboards and reports of business metrics.

By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data. Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

There is a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.Note that when using Auto Loader with automatic schema inference and evolution, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below. Databricks Auto Loader can automatically process files as they land in your cloud object stores. 

| argument | what it is | how it's used |
| --- | --- | --- |
| **`data_source`** | The directory of the source data | Auto Loader will detect new files as they arrive in this location and queue them for ingestion; passed to the **`.load()`** method |
| **`source_format`** | The format of the source data |  While the format for all Auto Loader queries will be **`cloudFiles`**, the format of the source data should always be specified for the **`cloudFiles.format`** option |
| **`table_name`** | The name of the target table | Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the **`.table()`** method. Note that you can either append to an existing table or create a new table |
 | **`checkpoint_directory`** | The location for storing metadata about the stream | This argument is passed to the **`checkpointLocation`** and **`cloudFiles.schemaLocation`** options. Checkpoints keep track of streaming progress, while the schema location tracks updates to the fields in the source dataset |
 
 ```
 def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query
```

Because Auto Loader uses Spark Structured Streaming to load data incrementally, the code above doesn't appear to finish executing. We can think of this as a **continuously active query**. This means that as soon as new data arrives in our data source, it will be processed through our logic and loaded into our target table.

```
query = autoload_to_table(data_source = f"{DA.paths.working_dir}/tracker",
                          source_format = "csv",
                          table_name = "target_table",
                          checkpoint_directory = f"{DA.paths.checkpoints}/target_table")
```

we define a helper function that prevents our notebook from executing the next cell just long enough to ensure data has been written out by a given streaming query. This code should not be necessary in a production job. Helper Function for Streaming combine streaming functions with batch and streaming queries against the results of those operations.


```
def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)
```

### Query Target Table

Once data has been ingested to Delta Lake with Auto Loader, users can interact with it the same way they would any table.

```
%sql
SELECT * FROM target_table
```
 
### Datasets Used
This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.

#### Recordings
The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
| Field | Type |
| --- | --- |
| device_id | int |
| mrn | long |
| time | double |
| heartrate | double |
 
#### PII
These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
| Field | Type |
| ---   | --- |
| mrn   | long |
| name  | string |

### Bronze Table: Ingesting Raw JSON Recordings
Below, we configure a read on a raw JSON source using Auto Loader with schema inference.

Note that while you need to use the Spark DataFrame API to set up an incremental read, once configured you can immediately register a temp view to leverage Spark SQL for streaming transformations on your data.
 **NOTE**: For a JSON data source, Auto Loader will default to inferring each column as a string. Here, we demonstrate specifying the data type for the **`time`** column using the **`cloudFiles.schemaHints`** option. Note that specifying improper types for a field will result in null values.

```
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))
```
Here, we'll enrich our raw data with additional metadata describing the source file and the time it was ingested. This additional metadata can be ignored during downstream processing while providing useful information for troubleshooting errors if corrupt data is encountered.

```
%sql
CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
SELECT *, current_timestamp() receipt_time, input_file_name() source_file
FROM recordings_raw_temp
```

The code below passes our enriched raw data back to PySpark API to process an incremental write to a Delta Lake table.

```
(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))
```

Trigger another file arrival with the following cell and you'll see the changes immediately detected by the streaming query you've written.
`DA.data_factory.load()`

### Load Static Lookup Table

The ACID guarantees that Delta Lake brings to your data are managed at the table level, ensuring that only fully successfully commits are reflected in your tables. If you choose to merge these data with other data sources, be aware of how those sources version data and what sort of consistency guarantees they have.

In this simplified demo, we are loading a static CSV file to add patient data to our recordings. In production, we could use Databricks' <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> feature to keep an up-to-date view of these data in our Delta Lake.

```
(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.datasets}/healthcare/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))


%sql
SELECT * FROM pii
```
### Silver Table: Enriched Recording Data

As a second hop in our silver level, we will do the follow enrichments and checks:
 - Our recordings data will be joined with the PII to add patient names
 - The time for our recordings will be parsed to the format **`'yyyy-MM-dd HH:mm:ss'`** to be human-readable
 - We will exclude heart rates that are <= 0, as we know that these either represent the absence of the patient or an error in transmission

```
(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))
```

```
%sql
CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
FROM bronze_tmp a
INNER JOIN pii b
ON a.mrn = b.mrn
WHERE heartrate > 0)
```

```
(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))
```

Trigger another new file and wait for it propagate through both previous queries.

```
%sql
SELECT COUNT(*) FROM recordings_enriched

DA.data_factory.load()
```

### Gold Table: Daily Averages

Here we read a stream of data from **`recordings_enriched`** and write another stream to create an aggregate gold table of daily averages for each patient.

```
(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

%sql
CREATE OR REPLACE TEMP VIEW patient_avg AS (
SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
FROM recordings_enriched_temp
GROUP BY mrn, name, date_trunc("DD", time))
```

Note that we're using **`.trigger(availableNow=True)`** below. This provides us the ability to continue to use the strengths of Structured Streaming while triggering this job one-time to process all available data in micro-batches. To recap, these strengths include:
 - exactly once end-to-end fault tolerant processing
- automatic detection of changes in upstream data sources
 
If we know the approximate rate at which our data grows, we can appropriately size the cluster we schedule for this job to ensure fast, cost-effective processing. The customer will be able to evaluate how much updating this final aggregate view of their data costs and make informed decisions about how frequently this operation needs to be run.

Downstream processes subscribing to this table do not need to re-run any expensive aggregations. Rather, files just need to be de-serialized and then queries based on included fields can quickly be pushed down against this already-aggregated source.

```
(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))
```

#### Important Considerations for complete Output with Delta

When using **`complete`** output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.

**NOTE**: Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignoring Updates and Deletes</a>.

The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

```
 %sql
SELECT * FROM daily_patient_avg
```

Note the above table includes all days for all users. If the predicates for our ad hoc queries match the data encoded here, we can push down our predicates to files at the source and very quickly generate more limited aggregate views.

```
 %sql
SELECT * 
FROM daily_patient_avg
WHERE date BETWEEN "2020-01-17" AND "2020-01-31"
```

### Process Remaining Records

The following cell will land additional files for the rest of 2020 in your source directory. You'll be able to see these process through the first 3 tables in your Delta Lake, but will need to re-run your final query to update your **`daily_patient_avg`** table, since this query uses the trigger available now syntax.

`DA.data_factory.load(continuous=True)`

### Wrapping Up
Finally, make sure all streams are stopped.

### Additional Topics & Resources

* <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Table Streaming Reads and Writes</a>
* <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>
* <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
* <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>
* <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
* <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Create a Kafka Source Stream</a>
