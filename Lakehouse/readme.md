# Implement Azure Databricks Lakehouse and build incremental workflows

We choose Azure Databricks to build the data engineering and analytics platfrom in the Azure cloud. 
Before we implement the Lakehouse platform we need to explore the external data from the ingested raw data by ADF in the Azure Data Lake, 
then we can use Auto Loader function to build incremental ETL pipelines as the multi-hop architecture - "Boronze", "Silver" and "Gold" data layer.

 - Explore and analyss external data from "Source" in Azure Data Lake
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

There is a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.Note that when using Auto Loader with automatic schema inference and evolution, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below.

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
