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


##  Query data in the lake using Azure Synapse serverless SQL pools 

Azure Synapse serverless SQL pool is tailored for querying the data in the lake. It supports querying CSV, JSON, and Parquet file formats directly.

--To craft queries to read a file (with specific format) and multiple files or folders. 

--To extract specific data out of the files you are interested in. 

--To query the different file types that can be stored in a data lake.

*  Query a CSV file using Azure Synapse serverless SQL pools

*  Query a Parquet file using Azure Synapse serverless SQL pools

*  Query a JSON file using Azure Synapse serverless SQL pools

*  Query multiple files and folders using Azure Synapse serverless SQL pools

*  Storage considerations when using Azure Synapse serverless SQL pools
  
  <a href="./sqlscript/querydatausingserverlesspool.sql">Examples</a>
  
## Create metadata objects in Azure Synapse serverless SQL pools

To query data or optimize your existing data transformation pipeline through Azure Synapse serverless SQL pools.

* Create databases in Azure Synapse serverless SQL pools
  *  To create a new database as an Azure Synapse serverless SQL pool, go to the Data hub, click + button and select SQL database.
  *  Choose one of the following two pool type options: Serverless/Dedicated
  *  Select Serverless if it isn’t already selected, type in a database name and press the button Create.
  *  Also, you can execute the following Transact-SQL statement in the serverless SQL pool:
  *  `CREATE DATABASE [YourDatabaseName]`
* Create and manage credentials in Azure Synapse serverless SQL pools
  * Azure Synapse serverless SQL pool accesses the storage to read the files using credentials. 
  * There are 3 types of credentials that are supported: Azure Active Directory pass-through, Managed Identity, Shared access signature (SAS).
  * To explicitly specify an identity of a logged in user, you need to create a database scoped credential as follows:
  
    `CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]`
    
    `WITH IDENTITY='User Identity'`
    
  * To instruct the serverless SQL pool to use a managed identity, create a credential as follows.
  
    `CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]`
    
    `WITH IDENTITY='Managed Identity'`
    
  * A Shared access signature (SAS) is a storage feature, that enables you to give a time bound permissions at a storage account file system or directory level
  
    `CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]`
    
    `WITH IDENTITY='SHARED ACCESS SIGNATURE',   
SECRET = 'sv=2018-03-28&ss=bf&srt=sco&sp=rl&st=2019-10-14T12%3A10%3A25Z&se=2061-12-31T12%3A10%3A00Z&sig=KlSU2ullCscyTS0An0nozEpo4tO5JAgGBvw%2FJX2lguw%3D'  `

* Create external data sources in Azure Synapse serverless SQL pools

  * An external data source is used to define the location of the data and the credential that should be used to access it. You can create external data source to a public storage account as follows:
  
  `CREATE EXTERNAL DATA SOURCE YellowTaxi 
   WITH ( LOCATION = 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/')`

  * If the storage account is protected, you must specify which credentials should be used like this:
  
  `CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH ( 
    LOCATION = 'https://sqlondemandstorage.blob.core.windows.net', 
    CREDENTIAL = sqlondemand 
   );`
   
   *  External data source are typically used in the OPENROWSET function or as part of the external table definition.
  
* Create external tables in Azure Synapse serverless SQL pools

  External tables are useful when you want to control access to external data in serverless SQL pools and is commonly used in PolyBase activities. If you want to use tools, such as Power BI, in conjunction with serverless SQL pools, then external tables are needed. External tables can access two types of storage:

  * Public storage where users access public storage files.
  
  * Protected storage where users access storage files using SAS credential, Azure AD identity, or the Managed Identity of an Azure Synapse workspace.
  
  * Example: 
  
  --First step is to create a database where the tables will be created. 
  
  --Then initialize the objects by executing the following setup script on that database. 
  
  https://sqlondemandstorage.blob.core.windows.net Azure storage account. DATABASE SCOPED CREDENTIAL sqlondemand that enables access to SAS-protected
  
  `CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]`
  
  `WITH IDENTITY='SHARED ACCESS SIGNATURE',   
   SECRET = 'sv=2018-03-28&ss=bf&srt=sco&sp=rl&st=2019-10-14T12%3A10%3A25Z&se=2061-12-31T12%3A10%3A00Z&sig=KlSU2ullCscyTS0An0nozEpo4tO5JAgGBvw%2FJX2lguw%3D'`
   
   `CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH (` 
   
   `LOCATION = 'https://sqlondemandstorage.blob.core.windows.net', `
   
    `CREDENTIAL = sqlondemand `
    
   `); `
   `GO `
   
   `CREATE EXTERNAL DATA SOURCE YellowTaxi` 
   
   `WITH ( LOCATION = 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/')`
  
   --Next you will create a file format named QuotedCSVWithHeaderFormat and FORMAT_TYPE of parquet that defines two file types. CSV and parquet.
   
   `CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeaderFormat `
   
   `WITH (`
   
    `FORMAT_TYPE = DELIMITEDTEXT, `
    
    `FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2   ) `
    
    `); `
    
   `GO `
   
   `CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (  FORMAT_TYPE = PARQUET );`
   
   --Create an external table on protected data
   
   With the database scoped credential, external data source, and external file format defined, you can create external tables that access data on an Azure storage account that allows access to users with some Azure AD identity or SAS key. You can create external tables the same way you create regular SQL Server external tables. 
   
   `USE [mydbname]; 
GO 
CREATE EXTERNAL TABLE populationExternalTable 
( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, 
    [population] bigint 
) 
WITH ( 
    LOCATION = 'csv/population/population.csv', 
    DATA_SOURCE = sqlondemanddemo, 
    FILE_FORMAT = QuotedCSVWithHeaderFormat 
);`
   
* Create views in Azure Synapse serverless SQL pools

  *  

## Work with Data Warehouses using Azure Synapse Analytics by developer features
### Work With Windowning functions
*  Window Functions
   *  Ranking functions
   *  Aggregate functions
   *  Analytic functions
   *  NEXT VALUE FOR function
 
<a href="https://docs.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql?view=sql-server-ver15">Windowning Functions</a>

*  Analytics Functions
   *  PERCENTILE_CONT and PERCENTILE_DISC<br>
   These functions may not return the same value. PERCENTILE_CONT interpolates the appropriate value, which may or may not exist in the data set, while PERCENTILE_DISC always returns an actual value from the set. To explain further, PERCENTILE_DISC computes a specific percentile for sorted values in an entire rowset or within a rowset's distinct partitions.The 0.5 value passed to the percentile functions computes the 50th percentile, or the median, of the downloads. The WITHIN GROUP expression specifies a list of values to sort and compute the percentile over. Only one ORDER BY expression is allowed, and the default sort order is ascending. The OVER clause divides the FROM clause's result set into partitions
   *  LAG analytic function<br>
   This function accesses data from a previous row in the same result set without the use of a self-join. LAG provides access to a row at a given physical offset that comes before the current row. We use this analytic function to compare values in the current row with values in a previous row.
   
<a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/analytic-functions-transact-sql?view=sql-server-ver15">Analytic Functions (Transact-SQL)</a>

*  The ROWS and RANGE clauses 
   *  They further limit the rows within the partition by specifying start and end points within the partition. This is done by specifying a range of rows with respect to the current row either by logical association or physical association. Physical association is achieved by using the ROWS clause.
   *  To use the FIRST_VALUE analytic function to retrieve the book title with the fewest downloads, as indicated by the ROWS UNBOUNDED PRECEDING clause over the Country partition. The UNBOUNDED PRECEDING option set the window start to the first row of the partition, giving us the title of the book with the fewest downloads for the country within the partition.

  <a href="./sqlscript/createdatawarehouses.sql">Examples</a>
  
### Work with approximate execution
*  To perform exploratory data analysis to gain an understanding of the data that they are working with. 
Exploratory data analysis can involve querying metadata about the data that is stored within the database, to running queries to provide a statistics information about the data such as average values for a column, through to distinct counts. Some of the activities can be time consuming, especially on large data sets.
*  Azure Synapse Analytics supports Approximate execution using Hyperlog accuracy to reduce latency when executing queries with large datasets. 
   Approximate execution is used to speed up the execution of queries with a compromise for a small reduction in accuracy. So if it takes too long to get basic information about the data in a large data set as you are exploring data of a big data set, then you can use the HyperLogLog accuracy and will return a result with a 2% accuracy of true cardinality on average. This is done by using the `APPROX_COUNT_DISTINCT` Transact-SQL function

### Work with JSON data in SQL pools
*  Synapse dedicated SQL Pools supports JSON format data to be stored using standard NVARCHAR table columns. 
*  The JSON format enables representation of complex or hierarchical data structures in tables. 
*  It allows to transform arrays of JSON objects into table format. The performance of JSON data can be optimized by using columnstore indexes and memory optimized tables.

   **Insert JSON data** - JSON data can be inserted using the usual T-SQL INSERT statements.
   
   **Read JSON data** - JSON data can be read using the following T-SQL functions and provides the ability to perform aggregation and filter on JSON values.
   
     -- ISJSON – verify if text is valid JSON
     
     -- JSON_VALUE – extract a scalar value from a JSON string
     
     -- JSON_QUERY – extract a JSON object or array from a JSON string
     
   **Modify JSON data** - JSON data can be modified and queried using the following T-SQL functions providing ability to update JSON string using T-SQL and convert hierarchical data into flat tabular structure.
   
     -- JSON_MODIFY – modifies a value in a JSON string
     
     -- OPENJSON – convert JSON collection to a set of rows and columns
     
     You can also query JSON files using SQL serverless. The query's objective is to read the following type of JSON files using **OPENROWSET**.

     -- Standard JSON files where multiple JSON documents are stored as a JSON array.
     
     -- Line-delimited JSON files, where JSON documents are separated with new-line character. Common extensions for these types of files are jsonl, ldjson, and ndjson.

     -- <a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql">OPENJSON</a>
     
     -- <a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/openrowset-transact-sql?view=sql-server-ver15">OPENROWSET</a>

### Encapsulate transact-SQL logic with stored procedures

*  Azure Synapse SQL pools support placing complex data processing logic into Stored procedures. 
*  Stored procedures are great way of encapsulating one or more SQL statements or a reference to a Microsoft .NET framework Common Language Runtime (CLR) method.
*  Stored procedures can accept input parameters and return multiple values in the form of output parameters to the calling program. 
*  In the context of serverless SQL pools, you will **perform data transformation using CREATE EXTERNAL TABLE AS SELECT (CETAS)** statement as shown in the following <a href="./sqlscript/createdatawarehouses.sql">Examples</a>.
*  In addition to encapsulating Transact-SQL logic, stored procedures also provide the following additional benefits:
   *  Reduces client to server network traffic
   *  Provides a security boundary
   *  Eases maintenance
   *  Improved performance

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

 
 # Manage and monitor data warehouse activities in Azure Synapse Analytics 
 
 ### Scale compute resources in Azure Synapse Analytics
 
 *  You can scale a Synapse SQL pool either through the Azure portal, Azure Synapse Studio or programmatically using TSQL or PowerShell.
 
 **SQL**
 
 `ALTER DATABASE mySampleDataWarehouse`
 
 `MODIFY (SERVICE_OBJECTIVE = 'DW300c');`
 
 **PowerShell**
 
 `Set-AzSqlDatabase -ResourceGroupName "resourcegroupname" -DatabaseName "mySampleDataWarehouse" -ServerName "sqlpoolservername" -RequestedServiceObjectiveName "DW300c"`
 
 * Scaling Apache Spark pools in Azure Synapse Analytics
 
   *  Apache Spark pools for Azure Synapse Analytics uses an Autoscale feature that automatically scales the number of nodes in a cluster instance up and down.
 
   *  During the creation of a new Spark pool, a minimum and maximum number of nodes can be set when Autoscale is selected. 
 
   *  Autoscale then monitors the resource requirements of the load and scales the number of nodes up or down. 
 
 
 ### Use dynamic management views to identify and troubleshoot query performance
 
 *  Dynamic Management Views provide a programmatic experience for monitoring the Azure Synapse Analytics SQL pool activity by using the Transact-SQL language. 
 
 *  The views that are provided, not only enable you to troubleshoot and identify performance bottlenecks with the workloads working on your system, but they are also used by other services such as Azure Advisor to provide recommendations about Azure Synapse Analytics.

 *  There are over 90 Dynamic Management Views that can queried against dedicated SQL pools to retrieve information about the following areas of the service:

     *  Connection information and activity
 
     *  SQL execution requests and queries
 
     *  Index and statistics information
 
     *  Resource blocking and locking activity
 
     * Data movement service activity

     * Errors
 
 -- All logins to your data warehouse are logged to sys.dm_pdw_exec_sessions. The session_id is the primary key and is assigned sequentially for each new logon.
 
 -- Other Active Connections
 
`SELECT * FROM sys.dm_pdw_exec_sessions where status <> 'Closed' and session_id <> session_id();`
 
 *  Monitor query execution
 
   *  All queries executed on SQL pool are logged to sys.dm_pdw_exec_requests. 
 
   *  The request_id uniquely identifies each query and is the primary key for this DMV. 
 
   *  The request_id is assigned sequentially for each new query and is prefixed with QID, which stands for query ID. 
 
   *  Querying this DMV for a given session_id shows all queries for a given logon.
 
   --**Step 1** The first step is to identify the query you want to investigate
 
   --To simplify the lookup of a query in the sys.dm_pdw_exec_requests table, use LABEL to assign a comment to your query, which can be looked up in the sys.dm_pdw_exec_requests view.
 
   --**Step 2**  Use the Request ID to retrieve the queries distributed SQL (DSQL) plan from sys.dm_pdw_request_steps
 To investigate further details about a single step, the operation_type column of the long-running query step and note the Step Index:

   --Proceed with Step 3 for SQL operations: OnOperation, RemoteOperation, ReturnOperation.

   --Proceed with Step 4 for Data Movement operations: ShuffleMoveOperation, BroadcastMoveOperation, TrimMoveOperation, PartitionMoveOperation, MoveOperation, CopyOperation.
 
 
<a href="https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-manage-monitor">Monitor your Azure Synapse Analytics dedicated SQL pool workload using DMVs</a>
