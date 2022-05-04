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

<code>cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir"</code><br>

<code>tableName = "dbo.DimCustomer"</code><br>

<code>customerDF = (spark.read</code><br>
  <code>.format("com.databricks.spark.sqldw")</code><br>
  <code>.option("url", jdbcURI)<br>
  <code>.option("tempDir", cacheDir)<br>
  <code>.option("forwardSparkAzureStorageCredentials", "true")</code><br>
  <code>.option("dbTable", tableName)</code><br>
  <code>.load())</code><br>

<code>customerDF.createOrReplaceTempView("customer_data")</code><br>
 
### Use SQL queries to count the number of rows in the Customer table and to display table metadata.

<code>%sql</code><br>
<code>select count(*) from customer_data</code><br>

 <code>%sql<br>
 <code>describe customer_data</code><br>
 
 Note that `CustomerKey` and `CustomerAlternateKey` use a very similar naming convention.

 <code>
 %sql<br>
  select CustomerKey, CustomerAlternateKey from customer_data limit 10;</code><br>
 
 In a situation in which we may be merging many new customers into this table, we can imagine that we may have issues with uniqueness with regard to the `CustomerKey`. Let us redefine `CustomerAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
 
 To do this we will define a UDF and use it to transform the `CustomerAlternateKey` column. Once this is done, we will write the updated Customer Table to a Staging table.
 
 **Note:** It is a best practice to update the Synapse instance via a staging table.
 
<code>import uuid</code><br>

<code>from pyspark.sql.types import StringType</code><br>
<code>from pyspark.sql.functions import udf</code><br>

<code>uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())</code><br>
<code>customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())</code><br>
<code>display(customerUpdatedDF)</code><br>
 
#### Use the Polybase Connector to Write to the Staging Table

<code>(customerUpdatedDF.write</code><br>
  <code>.format("com.databricks.spark.sqldw")</code><br>
  <code>.mode("overwrite")</code><br>
  <code>.option("url", jdbcURI)</code><br>
  <code>.option("forward_spark_azure_storage_credentials", "true")</code><br>
  <code>.option("dbtable", tableName + "Staging")</code><br>
  <code>.option("tempdir", cacheDir)</code><br>
 <code>.save())</code><br>

#### Read and Display Changes from Staging Table

<code>customerTempDF = (spark.read</code><br>
  <code>.format("com.databricks.spark.sqldw")</code><br>
  <code>.option("url", jdbcURI)</code><br>
  <code>.option("tempDir", cacheDir)</code><br>
  <code>.option("forwardSparkAzureStorageCredentials", "true")</code><br>
  <code>.option("dbTable", tableName + "Staging")</code><br>
 <code>.load())</code><br>

 <code>customerTempDF.createOrReplaceTempView("customer_temp_data")</code><br>

 <code> %sql</code><br>
 <code>select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;</code><br>
  
#  Analyze and optimize data warehouse storage in Azure Synapse Analytics 

`%%sql`
 -- Find data skew for a distributed table
 `DBCC PDW_SHOWSPACEUSED('dbo.FactInternetSales');`
  
## Analyze the space used by tables
1. **Open Synapse Studio.**
2. **Select the Develop hub.**
3. **From the Develop menu, select the + button (1) and choose SQL Script (2) from the context menu.**
4. **In the toolbar menu, connect to the SQLPool01 database to execute the query.**
5. **In the query window, replace the script with the following Database Console Command (DBCC):**
  `DBCC PDW_SHOWSPACEUSED('wwi_perf.Sale_Hash');`
6. **Analyze the number of rows in each distribution.**
  
  `SELECT TOP 1000
    CustomerId,
    count(*) as TransactionItemsCount
FROM
    [wwi_perf].[Sale_Hash]
GROUP BY
    CustomerId
ORDER BY
    count(*) DESC`
  
Now find the customers with the least sale transaction items:
  
`SELECT TOP 1000
    CustomerId,
    count(*) as TransactionItemsCount
FROM
    [wwi_perf].[Sale_Hash]
GROUP BY
    CustomerId
ORDER BY
    count(*) ASC`
  
 Let's find now the distribution of per-customer transaction item counts. Run the following query: 
  
`SELECT
    T.TransactionItemsCountBucket
    ,count(*) as CustomersCount
FROM
    (
        SELECT
            CustomerId,
            (count(*) - 16) / 100 as TransactionItemsCountBucket
        FROM
            [wwi_perf].[Sale_Hash]
        GROUP BY
            CustomerId
    ) T
GROUP BY
    T.TransactionItemsCountBucket
ORDER BY
    T.TransactionItemsCountBucket`
  
  
### Use a more advanced approach to understand table space usage
1. **Run the following script to create the `vTableSizes` view:**
  
  `CREATE VIEW [wwi_perf].[vTableSizes]
AS
WITH base
AS
(
SELECT
    GETDATE()                                                              AS  [execution_time]
    , DB_NAME()                                                            AS  [database_name]
    , s.name                                                               AS  [schema_name]
    , t.name                                                               AS  [table_name]
    , QUOTENAME(s.name)+'.'+QUOTENAME(t.name)                              AS  [two_part_name]
    , nt.[name]                                                            AS  [node_table_name]
    , ROW_NUMBER() OVER(PARTITION BY nt.[name] ORDER BY (SELECT NULL))     AS  [node_table_name_seq]
    , tp.[distribution_policy_desc]                                        AS  [distribution_policy_name]
    , c.[name]                                                             AS  [distribution_column]
    , nt.[distribution_id]                                                 AS  [distribution_id]
    , i.[type]                                                             AS  [index_type]
    , i.[type_desc]                                                        AS  [index_type_desc]
    , nt.[pdw_node_id]                                                     AS  [pdw_node_id]
    , pn.[type]                                                            AS  [pdw_node_type]
    , pn.[name]                                                            AS  [pdw_node_name]
    , di.name                                                              AS  [dist_name]
    , di.position                                                          AS  [dist_position]
    , nps.[partition_number]                                               AS  [partition_nmbr]
    , nps.[reserved_page_count]                                            AS  [reserved_space_page_count]
    , nps.[reserved_page_count] - nps.[used_page_count]                    AS  [unused_space_page_count]
    , nps.[in_row_data_page_count]
        + nps.[row_overflow_used_page_count]
        + nps.[lob_used_page_count]                                        AS  [data_space_page_count]
    , nps.[reserved_page_count]
    - (nps.[reserved_page_count] - nps.[used_page_count])
    - ([in_row_data_page_count]
            + [row_overflow_used_page_count]+[lob_used_page_count])        AS  [index_space_page_count]
    , nps.[row_count]                                                      AS  [row_count]
FROM
    sys.schemas s
INNER JOIN sys.tables t
    ON s.[schema_id] = t.[schema_id]
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1
INNER JOIN sys.pdw_table_distribution_properties tp
    ON t.[object_id] = tp.[object_id]
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.dm_pdw_nodes pn
    ON  nt.[pdw_node_id] = pn.[pdw_node_id]
INNER JOIN sys.pdw_distributions di
    ON  nt.[distribution_id] = di.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
LEFT OUTER JOIN (select * from sys.pdw_column_distribution_properties where distribution_ordinal = 1) cdp
    ON t.[object_id] = cdp.[object_id]
LEFT OUTER JOIN sys.columns c
    ON cdp.[object_id] = c.[object_id]
    AND cdp.[column_id] = c.[column_id]
WHERE pn.[type] = 'COMPUTE'
)
, size
AS
(
SELECT
[execution_time]
,  [database_name]
,  [schema_name]
,  [table_name]
,  [two_part_name]
,  [node_table_name]
,  [node_table_name_seq]
,  [distribution_policy_name]
,  [distribution_column]
,  [distribution_id]
,  [index_type]
,  [index_type_desc]
,  [pdw_node_id]
,  [pdw_node_type]
,  [pdw_node_name]
,  [dist_name]
,  [dist_position]
,  [partition_nmbr]
,  [reserved_space_page_count]
,  [unused_space_page_count]
,  [data_space_page_count]
,  [index_space_page_count]
,  [row_count]
,  ([reserved_space_page_count] * 8.0)                                 AS [reserved_space_KB]
,  ([reserved_space_page_count] * 8.0)/1000                            AS [reserved_space_MB]
,  ([reserved_space_page_count] * 8.0)/1000000                         AS [reserved_space_GB]
,  ([reserved_space_page_count] * 8.0)/1000000000                      AS [reserved_space_TB]
,  ([unused_space_page_count]   * 8.0)                                 AS [unused_space_KB]
,  ([unused_space_page_count]   * 8.0)/1000                            AS [unused_space_MB]
,  ([unused_space_page_count]   * 8.0)/1000000                         AS [unused_space_GB]
,  ([unused_space_page_count]   * 8.0)/1000000000                      AS [unused_space_TB]
,  ([data_space_page_count]     * 8.0)                                 AS [data_space_KB]
,  ([data_space_page_count]     * 8.0)/1000                            AS [data_space_MB]
,  ([data_space_page_count]     * 8.0)/1000000                         AS [data_space_GB]
,  ([data_space_page_count]     * 8.0)/1000000000                      AS [data_space_TB]
,  ([index_space_page_count]  * 8.0)                                   AS [index_space_KB]
,  ([index_space_page_count]  * 8.0)/1000                              AS [index_space_MB]
,  ([index_space_page_count]  * 8.0)/1000000                           AS [index_space_GB]
,  ([index_space_page_count]  * 8.0)/1000000000                        AS [index_space_TB]
FROM base
)
SELECT *
FROM size`
                        
2. **Run the following script to view the details about the structure of the tables in the wwi_perf schema:**
`SELECT
    database_name
,    schema_name
,    table_name
,    distribution_policy_name
,      distribution_column
,    index_type_desc
,    COUNT(distinct partition_nmbr) as nbr_partitions
,    SUM(row_count)                 as table_row_count
,    SUM(reserved_space_GB)         as table_reserved_space_GB
,    SUM(data_space_GB)             as table_data_space_GB
,    SUM(index_space_GB)            as table_index_space_GB
,    SUM(unused_space_GB)           as table_unused_space_GB
FROM
    [wwi_perf].[vTableSizes]
WHERE
    schema_name = 'wwi_perf'
GROUP BY
    database_name
,    schema_name
,    table_name
,    distribution_policy_name
,      distribution_column
,    index_type_desc
ORDER BY
    table_reserved_space_GB desc`
                        
                        
