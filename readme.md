# Data engineering with Azure Databricks

To harness the power of Apache Spark and powerful clusters running on the Azure Databricks platform to run large data engineering workloads in the cloud. Databricks Data Science & Engineering provides an interactive workspace that enables collaboration between data engineers, data scientists, and machine learning engineers. For a big data pipeline, the data (raw or structured) is ingested into Azure through Azure Data Factory in batches, or streamed near real-time using Apache Kafka, Event Hub, or IoT Hub. This data lands in a data lake for long term persisted storage, in Azure Blob Storage or Azure Data Lake Storage. As part of your analytics workflow, use Azure Databricks to read data from multiple data sources and turn it into breakthrough insights using Spark.

<img src="./azure-databricks.png" />

<a href="https://docs.microsoft.com/en-us/azure/databricks/scenarios/databricks-extract-load-sql-data-warehouse">Tutorial: Extract, transform, and load data by using Azure Databricks</a>

  * Create an Azure Databricks service.
  * Create a Spark cluster in Azure Databricks.
  * Create a file system in the Data Lake Storage Gen2 account.
  * Upload sample data to the Azure Data Lake Storage Gen2 account.
  * Create a service principal.
  * Extract data from the Azure Data Lake Storage Gen2 account.
  * Transform data in Azure Databricks.
  * Load data into Azure Synapse.


##  Describe Azure Databricks

Discover the capabilities of Azure Databricks and the Apache Spark notebook for processing huge files. Understand the Azure Databricks platform and identify the types of tasks well-suited for Apache Spark. Azure Databricks runs on top of a proprietary data processing engine called Databricks Runtime, an optimized version of Apache Spark. It allows up to 50x performance for Apache Spark workloads. Apache Spark is the core technology. Spark is an open-source analytics engine for large-scale data processing. It provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. In a nutshell: Azure Databricks offers a fast, easy, and collaborative Spark based analytics service. It is used to accelerate big data analytics, artificial intelligence, performant data lakes, interactive data science, machine learning, and collaboration.

When talking about the Azure Databricks workspace, we refer to two different things. The first reference is the logical Azure Databricks environment in which clusters are created, data is stored (via DBFS), and in which the server resources are housed. The second reference is the more common one used within the context of Azure Databricks. That is the special root folder for all of your organization's Databricks assets, including notebooks, libraries, and dashboards. 

*  You can use Apache Spark notebooks to:

   *  Read and process huge files and data sets
   *  Query, explore, and visualize data sets
   *  Join disparate data sets found in data lakes
   *  Train and evaluate machine learning models
   *  Process live streams of data
   *  Perform analysis on large graph data sets and social networks

##  Spark architecture fundamentals

Understand the architecture of an Azure Databricks Spark Cluster and Spark Jobs.

Azure Databricks provides a notebook-oriented Apache Spark as-a-service workspace environment. It is the most feature-rich hosted service available to run Spark workloads in Azure. Apache Spark is a unified analytics engine for large-scale data processing and machine learning. Suppose you work with Big Data as a data engineer or a data scientist, and you must process data that you can describe as having one or more of the following characteristics:

   *  High volume - You must process an extremely large volume of data and need to scale out your compute accordingly
   *  High velocity - You require streaming and real-time processing capabilities
   *  Variety - Your data types are varied, from structured relational data sets and financial transactions to unstructured data such as chat and SMS messages, IoT devices, images, logs, MRIs, etc.

These characteristics are oftentimes called the "3 Vs of Big Data". When it comes to working with Big Data in a unified way, whether you process it real time as it arrives or in batches, Apache Spark provides a fast and capable engine that also supports data science processes, like machine learning and advanced analytics.

In Databricks, the notebook interface is the driver program. This driver program contains the main loop for the program and creates distributed datasets on the cluster, then applies operations (transformations & actions) to those datasets. Driver programs access Apache Spark through a SparkSession object regardless of deployment location.

<img src="azure-databricks-architecture.png" />

The unit of distribution is a Spark Cluster. Every Cluster has a Driver and one or more executors. Work submitted to the Cluster is split into as many independent Jobs as needed. This is how work is distributed across the Cluster's nodes. Jobs are further subdivided into tasks. The input to a job is partitioned into one or more partitions. These partitions are the unit of work for each slot. In between tasks, partitions may need to be re-organized and shared over the network.

<img src="spark-cluster-tasks.png" />

Jobs & stages
  * Each parallelized action is referred to as a Job.
  * The results of each Job (parallelized/distributed action) is returned to the Driver.
  * Depending on the work required, multiple Jobs will be required.
  * Each Job is broken down into Stages.
  * This would be analogous to building a house (the job)
  * The first stage would be to lay the foundation.
  * The second stage would be to erect the walls.
  * The third stage would be to add the room.
  * Attempting to do any of these steps out of order just won't make sense, if not just impossible.

From a developer's and learner's perspective my primary focus is on...
  * The number of Partitions my data is divided into.
  * The number of Slots I have for parallel execution.
  * How many Jobs am I triggering?
  * And lastly the Stages those jobs are divided into.


##  Read and write data in Azure Databricks

Work with large amounts of data from multiple sources in different raw formats. Azure Databricks supports day-to-day data-handling functions, such as reads, writes, and queries.

* Using Spark to load table/file/DBFS data

    *  We can use Spark to load the table data by using the sql method with Python:
    `df = spark.sql("SELECT * FROM nyc_taxi_csv")`

    *  We can also read the data from the original files we've uploaded; or indeed from any other file available in the DBFS. The code is the same regardless of whether a file is local or in remote storage that was mounted, thanks to DBFS mountpoints (Python). Spark supports many different data formats, such as CSV, JSON, XML, Parquet, Avro, ORC and more.
    
    `df = spark.read.csv('dbfs:/FileStore/tables/nyc_taxi.csv', header=True, inferSchema=True)`
    
* DataFrame size/structure/contents
      
    *  To get the number of rows available in a DataFrame, we can use the count() method.
    
    `df.count`
    
    *  To get the schema metadata for a given DataFrame, we can use the printSchema() method. Each column in a given DataFrame has a name, a type, and a nullable flag.
    
    `df.printSchema`
    
   *  Spark has a built-in function that allows to print the rows inside a DataFrame: show()
   
    `df.show`
    
     `df.show(100, truncate=False) #show more lines, do not truncate`
     
     By default it will only show the first 20 lines in your DataFrame and it will truncate long columns. Additional parameters are available to override these settings.
     
* Query dataframes
  DataFrames allow the processing of huge amounts of data. Spark uses an optimization engine to generate logical queries. Data is distributed over your cluster and you get huge performance for massive amounts of data. Spark SQL is a component that introduced the DataFrames, which provides support for structured and semi-structured data. Spark has multiple interfaces (APIs) for dealing with DataFrames: 
   *  We have seen the .sql() method, which allows to run arbitrary SQL queries on table data. 
   *  Another option is to use the Spark domain-specific language for structured data manipulation, available in Scala, Java, Python, and R.
    
* Complete the following notebook: 
  *  Start working with the API documentation
  *  Introduce the class SparkSession and other entry points
  *  Introduce the class DataFrameReader
  *  Read data from: CSV without a Schema, CSV with a Schema
  *  Read data from: JSON without a Schema, JSON with a Schema
  *  Read data from: Parquet files without a schema, Parquet files with a schema
  *  Read data from tables/views, Write data to a Parquet file, Read the Parquet file back and display the results
  *  <a href="./notebook/Read Write Query with DataFrame.html">Examples</a>

##  Work with DataFrames in Azure Databricks

Your data processing in Azure Databricks is accomplished by defining DataFrames to read and process the Data. Learn how to perform data transformations in DataFrames and execute actions to display the transformed data.

Spark uses 3 different APIs: RDDs, DataFrames, and DataSets. The architectural foundation is the resilient distributed dataset (RDD). The DataFrame API was released as an abstraction on top of the RDD, followed later by the Dataset API. We'll only use DataFrames in our notebook examples. DataFrames are the distributed collections of data, organized into rows and columns. Each column in a DataFrame has a name and an associated type. Spark DataFrames can be created from various sources, such as CSV files, JSON, Parquet files, Hive tables, log tables, and external databases.

##  Describe lazy evaluation and other performance features in Azure Databricks

Understand the difference between a transform and an action, lazy and eager evaluations, Wide and Narrow transformations, and other optimizations in Azure Databricks.




##  Work with DataFrames columns in Azure Databricks

Use the DataFrame Column class in Azure Databricks to apply column-level transformations, such as sorts, filters and aggregations.



##  Work with DataFrames advanced methods in Azure Databricks

Use advanced DataFrame functions operations to manipulate data, apply aggregates, and perform date and time operations in Azure Databricks.

The Apache Spark DataFrame API provides a rich set of functions (select columns, filter, join, aggregate, and so on) that allow you to solve common data analysis problems efficiently. A complex operation where tables are joined, filtered, and restructured is easy to write, easy to understand, type safe, feels natural for people with prior sql experience, and comes with the added speed of parallel processing given by the Spark engine.

* To load or save data use read and write (Python):

  `df = spark.read.format('json').load('sample/trips.json')`
  
  `df.write.format('parquet').bucketBy(100, 'year', 'month').mode("overwrite").saveAsTable('table1'))`
  
 * To get the available data in a DataFrame use select:

   `df.select('*')`
  
   `df.select('tripDistance', 'totalAmount')`
  
* To extract the first rows, use take:

  `df.take(15)`
  
* To order the data, use the sort method:

   `df.sort(df.tripDistance.desc())`
  
* To combine the rows in multiple DataFrames use union:

   `df1.union(df2)`
  
   This operation is equivalent to UNION ALL in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by distinct().

   The dataframes must have the same structure/schema.

* To add or update columns use withColumn or withColumnRenamed:

   `df.withColumn('isHoliday', False)`
  
   `df.withColumnRenamed('isDayOff', 'isHoliday')`
   
*  To use aliases for the whole DataFrame or specific columns:

   `df.alias("myTrips")`
   
   `df.select(df.passengerCount.alias("numberOfPassengers"))`  
   
*  To create a temporary view:

   `df.createOrReplaceTempView("tripsView")`
   
*  To aggregate on the entire DataFrame without groups use agg:

   `df.agg({"age": "max"})` 
   
*  To do more complex queries, use filter, groupBy and join: 
   These join types are supported: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti. Note that filter is an alias for where.

   `people \`
   
   `.filter(people.age > 30) \`
   
   `.join(department, people.deptId == department.id) \`
   
   `.groupBy(department.name, "gender")`
   
   `.agg({"salary": "avg", "age": "max"})`
  
*  To use columns aggregations using windows:

   `w = Window.partitionBy("name").orderBy("age").rowsBetween(-1, 1)`
   
   `df.select(rank().over(w), min('age').over(window))`
   
*  To use a list of conditions for a column and return an expression use when:

   `df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()`
   
*  To check the presence of data use isNull or isNotNull:

   `df.filter(df.passengerCount.isNotNull())`
   
   `df.filter(df.totalAmount.isNull())`  
   
*  To clean the data use dropna, fillna or dropDuplicates:

   `df1.fillna(1) #replace nulls with specified value`
   
   `df2.dropna #drop rows containing null values`
   
   `df3.dropDuplicates #drop duplicate rows`  
   
*  To get statistics about the DataFrame use summary or describe:

   `df.summary().show()`
   
   `df.summary("passengerCount", "min", "25%", "75%", "max").show()`
   
   `df.describe(['age']).show()`  
   
   Available statistics are: Count, Mean, Stddev, Min, Max, Arbitrary approximate percentiles specified as a percentage (for example, 75%).

*  To find correlations between specific columns use corr. This operation currently only supports the Pearson Correlation Coefficient:

   `df.corr('tripDistance', 'totalAmount')`
   
   More information: for more information about the Spark API, see the <a href="https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame?azure-portal=true">DataFrame API</a> and the <a href="https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.Column?azure-portal=true">Column API</a> in the Spark documentation.
   
   
##  Describe platform architecture, security, and data protection in Azure Databricks

Understand the Azure Databricks platform components and best practices for securing your workspace through Databricks native features and by integrating with Azure services.



##  Build and query a Delta Lake

Learn how to use Delta Lake to create, append, and upsert data to Apache Spark tables, taking advantage of built-in reliability and optimizations.


##  Process streaming data with Azure Databricks structured streaming

Learn how Structured Streaming helps you process streaming data in real time, and how you can aggregate data over windows of time.



##  Describe Azure Databricks Delta Lake architecture

Use Delta Lakes as an optimization layer on top of blob storage to ensure reliability and low latency within unified Streaming + Batch data pipelines.



##  Create production workloads on Azure Databricks with Azure Data Factory

Azure Data Factory helps you create workflows that orchestrate data movement and transformation at scale. Integrate Azure Databricks into your production pipelines by calling notebooks and libraries.

* We can override the default language by specifying the language magic command %<language> at the beginning of a cell. The supported magic commands are:

  *   %python
  *   %r
  *   %scala
  *   %sql

  Notebooks also support a few auxiliary magic commands:

  *   %sh: Allows you to run shell code in your notebook
  *   %fs: Allows you to use dbutils filesystem commands
  *   %md: Allows you to include various types of documentation, including text, images, and mathematical formulas and equations.


##  Implement CI/CD with Azure DevOps

CI/CID isn't just for developers. Learn how to put Azure Databricks notebooks under version control in an Azure DevOps repo and build deployment pipelines to manage your release process.



##  Integrate Azure Databricks with Azure Synapse

Azure Databricks is just one of many powerful data services in Azure. Learn how to integrate with Azure Synapse Analytics as part of your data architecture.



##  Describe Azure Databricks best practices

Learn best practices for workspace administration, security, tools, integration, databricks runtime, HA/DR, and clusters in Azure Databricks.


# Build and operate machine learning solutions with Azure Databricks

Azure Databricks is a cloud-scale platform for data analytics and machine learning. In this learning path, you'll learn how to use Azure Databricks to explore, prepare, and model data; and integrate with Azure Machine Learning.

# Perform data science with Azure Databricks

Learn how to harness the power of Apache Spark and powerful clusters running on the Azure Databricks platform to run data science workloads in the cloud.
