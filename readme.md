# Reading and Writing to Synapse
 
 ## Objectives

 * Describe the connection architecture of Synapse and Spark
 * Configure a connection between Databricks and Synapse
 * Read data from Synapse
 * Write data to Synapse
 
 ### Azure Synapse
 - leverages massively parallel processing (MPP) to quickly run complex queries across petabytes of data
 - PolyBase T-SQL queries

 ##### Synapse Connector
 - uses Azure Blob Storage as intermediary
 - uses PolyBase in Synapse
 - enables MPP reads and writes to Synapse from Azure Databricks
 
 Note: The Synapse connector is more suited to ETL than to interactive queries. For interactive and ad-hoc queries, data should be extracted into a Databricks Delta table.
 
 ```
                            ┌─────────┐
       ┌───────────────────>│ STORAGE │<──────────────────┐
       │ Storage acc key /  │ ACCOUNT │ Storage acc key / │
       │ Managed Service ID └─────────┘ OAuth 2.0         │
       │                         │                        │
       │                         │ Storage acc key /      │
       │                         │ OAuth 2.0              │
       v                         v                 ┌──────v────┐
 ┌──────────┐              ┌──────────┐            │┌──────────┴┐
 │ Synapse  │              │  Spark   │            ││ Spark     │
 │ Analytics│<────────────>│  Driver  │<───────────>| Executors │
 └──────────┘  JDBC with   └──────────┘ Configured  └───────────┘
               username &               in Spark
               password
 ```
 
 #### SQL DW Connection
 
 Three connections are made to exchange queries and data between Databricks and Synapse
 1. **Spark driver to Synapse**
    - the Spark driver connects to Synapse via JDBC using a username and password
 2. **Spark driver and executors to Azure Blob Storage**
    - the Azure Blob Storage container acts as an intermediary to store bulk data when reading from or writing to Synapse
    - Spark connects to the Blob Storage container using the Azure Blob Storage connector bundled in Databricks Runtime
    - the URI scheme for specifying this connection must be wasbs
    - the credential used for setting up this connection must be a storage account access key
    - the account access key is set in the session configuration associated with the notebook that runs the command
    - this configuration does not affect other notebooks attached to the same cluster. `spark` is the SparkSession object provided in the notebook
 3. **Synapse to Azure Blob Storage**
    - Synapse also connects to the Blob Storage container during loading and unloading of temporary data
    - set `forwardSparkAzureStorageCredentials` to true
    - the forwarded storage access key is represented by a temporary database scoped credential in the Synapse instance
    - Synapse connector creates a database scoped credential before asking Synapse to load or unload data
    - then it deletes the database scoped credential once the loading or unloading operation is done.

 #### Enter Variables from Cloud Setup
 
 Before starting this lesson, you were guided through configuring Azure Synapse and deploying a Storage Account and blob container.
 
 In the cell below, enter the **Storage Account Name**, the **Container Name**, and the **Access Key** for the blob container you created.
 
 Also enter the JDBC connection string for your Azure Synapse instance. Make sure you substitute in your password as indicated within the generated string.

<code>
storageAccount = "name-of-your-storage-account"<br>
containerName = "data"<br>
accessKey = "your-storage-key"<br>
jdbcURI = ""<br>

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", accessKey)</code><br>

 #### Read from the Customer Table
 
 Next, use the Synapse Connector to read data from the Customer Table.
 
 Use the read to define a tempory table that can be queried.
 
 Note: 
 - the connector uses a caching directory on the Azure Blob Container.
 - `forwardSparkAzureStorageCredentials` is set to `true` so that the Synapse instance can access the blob for its MPP read via Polybase

 `cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir`

  `tableName = "dbo.DimCustomer"`

  `customerDF = (spark.read`
     `.format("com.databricks.spark.sqldw")`
     `.option("url", jdbcURI)`
     `.option("tempDir", cacheDir)`
     `.option("forwardSparkAzureStorageCredentials", "true")`
     `.option("dbTable", tableName)`
     `.load())`

  `customerDF.createOrReplaceTempView("customer_data")`<br>
 
 ###  Use SQL queries to count the number of rows in the Customer table and to display table metadata.

`%sql`
`select count(*) from customer_data`

 `%sql`
 `describe customer_data`
 
 Note that `CustomerKey` and `CustomerAlternateKey` use a very similar naming convention.

 
  `%sql`
  `select CustomerKey, CustomerAlternateKey from customer_data limit 10:`
 
 In a situation in which we may be merging many new customers into this table, we can imagine that we may have issues with uniqueness with regard to the `CustomerKey`. Let us redefine `CustomerAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
 
 To do this we will define a UDF and use it to transform the `CustomerAlternateKey` column. Once this is done, we will write the updated Customer Table to a Staging table.
 
 **Note:** It is a best practice to update the Synapse instance via a staging table.
 
<code>import uuid</code><br>

<code>from pyspark.sql.types import StringType</code><br>
<code>from pyspark.sql.functions import udf</code><br>

<code>uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())</code><br>
<code>customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())</code><br>
<code>display(customerUpdatedDF)</code><br>
 
### Use the Polybase Connector to Write to the Staging Table

<code>(customerUpdatedDF.write</code><br>
  <code>.format("com.databricks.spark.sqldw")</code><br>
  <code>.mode("overwrite")</code><br>
  <code>.option("url", jdbcURI)</code><br>
  <code>.option("forward_spark_azure_storage_credentials", "true")</code><br>
  <code>.option("dbtable", tableName + "Staging")</code><br>
  <code>.option("tempdir", cacheDir)</code><br>
 <code>.save())</code><br>

### Read and Display Changes from Staging Table

<code>customerTempDF = (spark.read</code><br>
  <code>.format("com.databricks.spark.sqldw")</code><br>
  <code>.option("url", jdbcURI)</code><br>
  <code>.option("tempDir", cacheDir)</code><br>
  <code>.option("forwardSparkAzureStorageCredentials", "true")</code><br>
  <code>.option("dbTable", tableName + "Staging")</code><br>
 <code>.load())</code><br>

 <code>customerTempDF.createOrReplaceTempView("customer_temp_data")</code><br>

 <code> %sql</code><br>
 <code>select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;</code><br>`

### Work With Windowning functions
*  Window Functions
   *  Ranking functions
   *  Aggregate functions
   *  Analytic functions
   *  NEXT VALUE FOR function
 
<a href="https://docs.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-ver15">Windowning Functions</a>

*  Analytics Functions
   *  PERCENTILE_CONT and PERCENTILE_DISC
   These functions may not return the same value. PERCENTILE_CONT interpolates the appropriate value, which may or may not exist in the data set, while PERCENTILE_DISC always returns an actual value from the set. To explain further, PERCENTILE_DISC computes a specific percentile for sorted values in an entire rowset or within a rowset's distinct partitions.The 0.5 value passed to the percentile functions computes the 50th percentile, or the median, of the downloads. The WITHIN GROUP expression specifies a list of values to sort and compute the percentile over. Only one ORDER BY expression is allowed, and the default sort order is ascending. The OVER clause divides the FROM clause's result set into partitions
   *  LAG analytic function
   This function accesses data from a previous row in the same result set without the use of a self-join. LAG provides access to a row at a given physical offset that comes before the current row. We use this analytic function to compare values in the current row with values in a previous row.
<a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/analytic-functions-transact-sql?view=sql-server-ver15">Analytic Functions (Transact-SQL)</a>


# Use data loading best practices in Azure Synapse Analytics 

### Load methods into Azure Synapse Analytics
1. **Data loads directly from Azure storage with transact-sql and the copy statement**
2. **Perform data loads using Azure synapse pipeline data flows.**
3. **Use polybase by defining external tables**
  
### Set-up dedicated data load accounts
1. **The first step is to connect to master and create a login.**
   <br>-- Connect to master
`CREATE LOGIN loader WITH PASSWORD = 'a123STRONGpassword!';`<br>
2. **Next, connect to the dedicated SQL pool and create a user.**
  <br>`-- Connect to the SQL pool
CREATE USER loader FOR LOGIN loader;
GRANT ADMINISTER DATABASE BULK OPERATIONS TO loader;
GRANT INSERT ON <yourtablename> TO loader;
GRANT SELECT ON <yourtablename> TO loader;
GRANT CREATE TABLE TO loader;
GRANT ALTER ON SCHEMA::dbo TO loader;

CREATE WORKLOAD GROUP DataLoads
WITH ( 
    MIN_PERCENTAGE_RESOURCE = 100
    ,CAP_PERCENTAGE_RESOURCE = 100
    ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100
    );

CREATE WORKLOAD CLASSIFIER [wgcELTLogin]
WITH (
        WORKLOAD_GROUP = 'DataLoads'
    ,MEMBERNAME = 'loader'
);`

### Simplify ingestion with the Copy Activity

#### Ingest data using PolyBase
1. **Create a new SQL script with the following to create the external data source. Be sure to replace YOURACCOUNT with the name of your ADLS Gen2 account:**
2. **Select Run from the toolbar menu to execute the SQL command.**
3. **In the query window, replace the script with the following to create the external file format and external data table.**
4. **Select Run from the toolbar menu to execute the SQL command.**
5. **In the query window, replace the script with the following to load the data into the wwi_staging.SalesHeap table.**

#### Simplify ingestion with the COPY activity
1. **In the query window, replace the script with the following to truncate the heap table and load data using the COPY statement.**
2. **Select Run from the toolbar menu to execute the SQL command.**
3. **In the query window, replace the script with the following to see how many rows were imported:**
4. **Select Run from the toolbar menu to execute the SQL command.**

##### Load a text file with non-standard row delimiters
One of the realities of data engineering, is that many times we need to process imperfect data. 
That is to say, data sources that contain invalid data formats, corrupted records, or custom configurations such as non-standard delimiters.

* Successfully load using COPY
  * One of the advantages COPY has over PolyBase is that it supports custom column and row delimiters.
  * Suppose you have a nightly process that ingests regional sales data from a partner analytics system and saves the files in the data lake. 
  * The text files use non-standard column and row delimiters where columns are delimited by a . and rows by a ,:
  * e.g. 20200421.114892.130282.159488.172105.196533,20200420.109934.108377.122039.101946.100712,20200419.253714.357583.452690.553447.653921
  * The data has the following fields: Date, NorthAmerica, SouthAmerica, Europe, Africa, and Asia. 
  * They must process this data and store it in Synapse Analytics.
  * To create the DailySalesCounts table and load data using the COPY statement. 
  * As before, be sure to replace YOURACCOUNT with the name of your ADLS Gen2 account:
  * `CREATE TABLE [wwi_staging].DailySalesCounts`<br>
    `(`<br>
        `[Date] [int]  NOT NULL,`<br>
        `[NorthAmerica] [int]  NOT NULL,`<br>
        `[SouthAmerica] [int]  NOT NULL,`<br>
        `[Europe] [int]  NOT NULL,`<br>
        `[Africa] [int]  NOT NULL,`<br>
        `[Asia] [int]  NOT NULL`<br>
    `)`<br>
`GO`<br>

**Note:** Replace <PrimaryStorage> with the workspace default storage account name.
 
`COPY INTO wwi_staging.DailySalesCounts`<br>
`FROM 'https://YOURACCOUNT.dfs.core.windows.net/wwi-02/campaign-analytics/dailycounts.txt'`<br>
`WITH (`<br>
    `FILE_TYPE = 'CSV',`<br>
    `FIELDTERMINATOR='.',`<br>
    `ROWTERMINATOR=','`<br>
`)`<br>
`GO`<br>
 
 * Attempt to load using PolyBase
   *  to create a new external file format, external table, and load data using PolyBase:
   *  `CREATE EXTERNAL FILE FORMAT csv_dailysales`<br>
`WITH (`<br>
    `FORMAT_TYPE = DELIMITEDTEXT,`<br>
   ` FORMAT_OPTIONS (`<br>
        `FIELD_TERMINATOR = '.',`<br>
        `DATE_FORMAT = '',`<br>
        `USE_TYPE_DEFAULT = False`<br>
    `)`<br>
`);`<br>
`GO`<br>

`CREATE EXTERNAL TABLE [wwi_external].DailySalesCounts`<br>
    `(`<br>
        `[Date] [int]  NOT NULL,`<br>
        `[NorthAmerica] [int]  NOT NULL,`<br>
        `[SouthAmerica] [int]  NOT NULL,`<br>
        `[Europe] [int]  NOT NULL,`<br>
        `[Africa] [int]  NOT NULL,`<br>
        `[Asia] [int]  NOT NULL`<br>
    `)`<br>
`WITH`<br>
    `(`<br>
        `LOCATION = '/campaign-analytics/dailycounts.txt',`<br>  
        `DATA_SOURCE = ABSS,`<br>
        `FILE_FORMAT = csv_dailysales`<br>
    `)  `<br>
`GO`<br>
`INSERT INTO [wwi_staging].[DailySalesCounts]`<br>
`SELECT *`<br>
`FROM [wwi_external].[DailySalesCounts]`<br>
 
 ###  <a href="analyze-optimize-performamce.md">Analyze and optimize data warehouse storage in Azure Synapse Analytics</a>
