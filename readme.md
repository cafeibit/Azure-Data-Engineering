# Work with Data Warehouses using Azure Synapse Analytics

 * <a href="#section0">Design tables in Azure Synapse Analytics</a>
 
 * <a href="#section1">Reading and Writing to Synapse</a>

 * <a href="#section2">Query data in the lake using Azure Synapse serverless SQL pools</a>

 * <a href="#section3">Create metadata objects in Azure Synapse serverless SQL pools</a>

 * <a href="#section4">Use Azure Synapse serverless SQL pools for transforming the data in the lake</a>

 * <a href="#section6">Serve the data from Azure Synapse serverless SQL pool</a>

 * <a href="#section7">Work with Data Warehouses using Azure Synapse Analytics by developer features</a>

 * <a href="#section8">Use data loading best practices in Azure Synapse Analytics</a>

 * <a href="#section9">Manage and monitor data warehouse activities in Azure Synapse Analytics</a>

# Perform data engineering with Azure Synapse Apache Spark Pools

To perform data engineering with Azure Synapse Apache Spark Pools, which enable you to boost the performance of big-data analytic applications by in-memory cluster computing.

 * <a href="#section2-1">Analyze data with Apache Spark in Azure Synapse Analytics</a>
 
 * <a href="#section2-2">Ingest data with Apache Spark notebooks in Azure Synapse Analytics</a>

 * <a href="#section2-3">Transform data with DataFrames in Apache Spark Pools in Azure Synapse Analytics</a>

 * <a href="#section5">Integrate SQL and Apache Spark pools in Azure Synapse Analytics</a>

 * <a href="#section2-5">Monitor and manage data engineering workloads with Apache Spark in Azure Synapse Analytics</a>
 
## <h2 id="section0">Design tables using Synapse SQL in Azure Synapse Analytics</h2>

Designing tables with dedicated SQL pool and serverless SQL pool.

Serverless SQL pool is a query service over the data in your data lake. It doesn't have local storage for data ingestion. Dedicated SQL pool represents a collection of analytic resources that are being provisioned when using Synapse SQL. The size of a dedicated SQL pool is determined by Data Warehousing Units (DWU).

**Common distribution methods for tables**

The table category often determines which option to choose for distributing the table.

```
Table category	   Recommended distribution option
Fact	             Use hash-distribution with clustered columnstore index. Performance improves when two hash tables are joined on the same distribution column.
Dimension	        Use replicated for smaller tables. If tables are too large to store on each Compute node, use hash-distributed.
Staging	          Use round-robin for the staging table. The load with CTAS is fast. Once the data is in the staging table, use INSERT...SELECT to move the data to                   production tables.
```

**Table partitions**

A partitioned table stores and performs operations on the table rows according to data ranges. For example, a table could be partitioned by day, month, or year. You can improve query performance through partition elimination, which limits a query scan to data within a partition. You can also maintain the data through partition switching. Since the data in SQL pool is already distributed, too many partitions can slow query performance. For more information, see Partitioning guidance. When partition switching into table partitions that are not empty, consider using the TRUNCATE_TARGET option in your ALTER TABLE statement if the existing data is to be truncated. The below code switches in the transformed daily data into the SalesFact overwriting any existing data.

`ALTER TABLE SalesFact_DailyFinalLoad SWITCH PARTITION 256 TO SalesFact PARTITION 256 WITH (TRUNCATE_TARGET = ON);`

**Columnstore indexes**

By default, dedicated SQL pool stores a table as a clustered columnstore index. This form of data storage achieves high data compression and query performance on large tables. The clustered columnstore index is usually the best choice, but in some cases a clustered index or a heap is the appropriate storage structure. A heap table can be especially useful for loading transient data, such as a staging table which is transformed into a final table. 

**Statistics**

The query optimizer uses column-level statistics when it creates the plan for executing a query. To improve query performance, it's important to have statistics on individual columns, especially columns used in query joins. Creating statistics happens automatically. Updating statistics doesn't happen automatically. Update statistics after a significant number of rows are added or changed. For example, update statistics after a load. 

Primary key and unique key
PRIMARY KEY is only supported when NONCLUSTERED and NOT ENFORCED are both used. UNIQUE constraint is only supported with NOT ENFORCED is used. Check Dedicated SQL pool table constraints.

**Commands for creating tables**

You can create a table as a new empty table. You can also create and populate a table with the results of a select statement. The following are the T-SQL commands for creating a table.
```
T-SQL                           Statement	Description
CREATE TABLE	                   | Creates an empty table by defining all the table columns and options.
CREATE EXTERNAL TABLE	          | Creates an external table. The definition of the table is stored in dedicated SQL pool. The table data is stored in Azure Blob storage or   Azure Data Lake Store.
CREATE TABLE AS SELECT	         | Populates a new table with the results of a select statement. The table columns and data types are based on the select statement results. To import data, this statement can select from an external table.
CREATE EXTERNAL TABLE AS SELECT	| Creates a new external table by exporting the results of a select statement to an external location. The location is either Azure Blob storage or Azure Data Lake Store.
```

**Aligning source data with dedicated SQL pool**

Dedicated SQL pool tables are populated by loading data from another data source. To perform a successful load, the number and data types of the columns in the source data must align with the table definition in the dedicated SQL pool. Getting the data to align might be the hardest part of designing your tables.

If data is coming from multiple data stores, you load the data into the dedicated SQL pool and store it in an integration table. Once data is in the integration table, you can use the power of dedicated SQL pool to perform transformation operations. Once the data is prepared, you can insert it into production tables.

**Unsupported table features**

Dedicated SQL pool supports many, but not all, of the table features offered by other databases. The following list shows some of the table features that aren't supported in dedicated SQL pool:

* Foreign key, Check Table Constraints
* Computed Columns
* Indexed Views
* Sequence
* Sparse Columns
* Surrogate Keys. Implement with Identity.
* Synonyms
* Triggers
* Unique Indexes
* User-Defined Types
* Table size queries

One simple way to identify space and rows consumed by a table in each of the 60 distributions, is to use DBCC PDW_SHOWSPACEUSED.

`DBCC PDW_SHOWSPACEUSED('dbo.FactInternetSales');`

However, using DBCC commands can be quite limiting. Dynamic management views (DMVs) show more detail than DBCC commands. Start by creating this view:

```
CREATE VIEW dbo.vTableSizes
AS
WITH base
AS
(
SELECT
 GETDATE()                                                             AS  [execution_time]
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
         + [row_overflow_used_page_count]+[lob_used_page_count])       AS  [index_space_page_count]
, nps.[row_count]                                                      AS  [row_count]
from
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
FROM size
;
```

**Table space summary**

This query returns the rows and space by table. It allows you to see which tables are your largest tables and whether they are round-robin, replicated, or hash-distributed. For hash-distributed tables, the query shows the distribution column.

```
SELECT
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
    dbo.vTableSizes
GROUP BY
     database_name
,    schema_name
,    table_name
,    distribution_policy_name
,      distribution_column
,    index_type_desc
ORDER BY
    table_reserved_space_GB desc
;
```

**Table space by distribution type**

```
SELECT
     distribution_policy_name
,    SUM(row_count)                as table_type_row_count
,    SUM(reserved_space_GB)        as table_type_reserved_space_GB
,    SUM(data_space_GB)            as table_type_data_space_GB
,    SUM(index_space_GB)           as table_type_index_space_GB
,    SUM(unused_space_GB)          as table_type_unused_space_GB
FROM dbo.vTableSizes
GROUP BY distribution_policy_name
;
```

**Table space by index type**

```
SELECT
     index_type_desc
,    SUM(row_count)                as table_type_row_count
,    SUM(reserved_space_GB)        as table_type_reserved_space_GB
,    SUM(data_space_GB)            as table_type_data_space_GB
,    SUM(index_space_GB)           as table_type_index_space_GB
,    SUM(unused_space_GB)          as table_type_unused_space_GB
FROM dbo.vTableSizes
GROUP BY index_type_desc
;
Distribution space summary
SQL

Copy
SELECT
    distribution_id
,    SUM(row_count)                as total_node_distribution_row_count
,    SUM(reserved_space_MB)        as total_node_distribution_reserved_space_MB
,    SUM(data_space_MB)            as total_node_distribution_data_space_MB
,    SUM(index_space_MB)           as total_node_distribution_index_space_MB
,    SUM(unused_space_MB)          as total_node_distribution_unused_space_MB
FROM dbo.vTableSizes
GROUP BY     distribution_id
ORDER BY    distribution_id
;
```

### Guidance for designing distributed tables using dedicated SQL pool in Azure Synapse Analytics

**Consider using a hash-distributed table when:**

* The table size on disk is more than 2 GB.
* The table has frequent insert, update, and delete operations.

**Consider using the round-robin distribution for your table in the following scenarios:**

* When getting started as a simple starting point since it is the default
* If there is no obvious joining key
* If there is no good candidate column for hash distributing the table
* If the table does not share a common join key with other tables
* If the join is less significant than other joins in the query
* When the table is a temporary staging table

#### Choose a distribution column with data that distributes evenly

Data stored in the distribution column can be updated. Updates to data in the distribution column could result in data shuffle operation. Choosing a distribution column is an important design decision since the values in this column determine how the rows are distributed. The best choice depends on several factors, and usually involves tradeoffs. Once a distribution column is chosen, you cannot change it.

If you didn't choose the best column the first time, you can use CREATE TABLE AS SELECT (CTAS) to re-create the table with a different distribution column.
For best performance, all of the distributions should have approximately the same number of rows. When one or more distributions have a disproportionate number of rows, some distributions finish their portion of a parallel query before others. Since the query can't complete until all distributions have finished processing, each query is only as fast as the slowest distribution.

* Data skew means the data is not distributed evenly across the distributions
* Processing skew means that some distributions take longer than others when running parallel queries. This can happen when the data is skewed.

To balance the parallel processing, select a distribution column that:

* **Has many unique values**. The column can have duplicate values. All rows with the same value are assigned to the same distribution. Since there are 60 distributions, some distributions can have > 1 unique values while others may end with zero values.
* **Does not have NULLs, or has only a few NULLs**. For an extreme example, if all values in the column are NULL, all the rows are assigned to the same distribution. As a result, query processing is skewed to one distribution, and does not benefit from parallel processing.
* **Is not a date column**. All data for the same date lands in the same distribution. If several users are all filtering on the same date, then only 1 of the 60 distributions do all the processing work.

**Choose a distribution column that minimizes data movement**

To get the correct query result queries might move data from one Compute node to another. Data movement commonly happens when queries have joins and aggregations on distributed tables. Choosing a distribution column that helps minimize data movement is one of the most important strategies for optimizing performance of your dedicated SQL pool.

**To minimize data movement, select a distribution column that:**

* Is used in JOIN, GROUP BY, DISTINCT, OVER, and HAVING clauses. When two large fact tables have frequent joins, query performance improves when you distribute both tables on one of the join columns. When a table is not used in joins, consider distributing the table on a column that is frequently in the GROUP BY clause.
* Is not used in WHERE clauses. This could narrow the query to not run on all the distributions.
* Is not a date column. WHERE clauses often filter by date. When this happens, all the processing could run on only a few distributions.

**What to do when none of the columns are a good distribution column**

If none of your columns have enough distinct values for a distribution column, you can create a new column as a composite of one or more values. To avoid data movement during query execution, use the composite distribution column as a join column in queries.

Once you design a hash-distributed table, the next step is to load data into the table. For loading guidance, see Loading overview.

How to tell if your distribution column is a good choice
After data is loaded into a hash-distributed table, check to see how evenly the rows are distributed across the 60 distributions. The rows per distribution can vary up to 10% without a noticeable impact on performance.

**Determine if the table has data skew**

A quick way to check for data skew is to use DBCC PDW_SHOWSPACEUSED. The following SQL code returns the number of table rows that are stored in each of the 60 distributions. For balanced performance, the rows in your distributed table should be spread evenly across all the distributions.

```
-- Find data skew for a distributed table
DBCC PDW_SHOWSPACEUSED('dbo.FactInternetSales');
```

**To identify which tables have more than 10% data skew:**

1. Create the view dbo.vTableSizes that is shown in the Tables overview article.
2. Run the following query:

```
select *
from dbo.vTableSizes
where two_part_name in
    (
    select two_part_name
    from dbo.vTableSizes
    where row_count > 0
    group by two_part_name
    having (max(row_count * 1.000) - min(row_count * 1.000))/max(row_count * 1.000) >= .10
    )
order by two_part_name, row_count
;
```

**Check query plans for data movement**

A good distribution column enables joins and aggregations to have minimal data movement. This affects the way joins should be written. To get minimal data movement for a join on two hash-distributed tables, one of the join columns needs to be the distribution column. When two hash-distributed tables join on a distribution column of the same data type, the join does not require data movement. Joins can use additional columns without incurring data movement.

**To avoid data movement during a join:**

* The tables involved in the join must be hash distributed on one of the columns participating in the join.
* The data types of the join columns must match between both tables.
* The columns must be joined with an equals operator.
* The join type may not be a `CROSS JOIN`.
* 
To see if queries are experiencing data movement, you can look at the query plan.

**Resolve a distribution column problem**

It is not necessary to resolve all cases of data skew. Distributing data is a matter of finding the right balance between minimizing data skew and data movement. It is not always possible to minimize both data skew and data movement. Sometimes the benefit of having the minimal data movement might outweigh the impact of having data skew.

To decide if you should resolve data skew in a table, you should understand as much as possible about the data volumes and queries in your workload. You can use the steps in the Query monitoring article to monitor the impact of skew on query performance. Specifically, look for how long it takes large queries to complete on individual distributions.

Since you cannot change the distribution column on an existing table, the typical way to resolve data skew is to re-create the table with a different distribution column.

**Re-create the table with a new distribution column**

This example uses CREATE TABLE AS SELECT to re-create a table with a different hash distribution column.

```
CREATE TABLE [dbo].[FactInternetSales_CustomerKey]
WITH (  CLUSTERED COLUMNSTORE INDEX
     ,  DISTRIBUTION =  HASH([CustomerKey])
     ,  PARTITION       ( [OrderDateKey] RANGE RIGHT FOR VALUES (   20000101, 20010101, 20020101, 20030101
                                                                ,   20040101, 20050101, 20060101, 20070101
                                                                ,   20080101, 20090101, 20100101, 20110101
                                                                ,   20120101, 20130101, 20140101, 20150101
                                                                ,   20160101, 20170101, 20180101, 20190101
                                                                ,   20200101, 20210101, 20220101, 20230101
                                                                ,   20240101, 20250101, 20260101, 20270101
                                                                ,   20280101, 20290101
                                                                )
                        )
    )
AS
SELECT  *
FROM    [dbo].[FactInternetSales]
OPTION  (LABEL  = 'CTAS : FactInternetSales_CustomerKey')
;
```

```
--Create statistics on new table
CREATE STATISTICS [ProductKey] ON [FactInternetSales_CustomerKey] ([ProductKey]);
CREATE STATISTICS [OrderDateKey] ON [FactInternetSales_CustomerKey] ([OrderDateKey]);
CREATE STATISTICS [CustomerKey] ON [FactInternetSales_CustomerKey] ([CustomerKey]);
CREATE STATISTICS [PromotionKey] ON [FactInternetSales_CustomerKey] ([PromotionKey]);
CREATE STATISTICS [SalesOrderNumber] ON [FactInternetSales_CustomerKey] ([SalesOrderNumber]);
CREATE STATISTICS [OrderQuantity] ON [FactInternetSales_CustomerKey] ([OrderQuantity]);
CREATE STATISTICS [UnitPrice] ON [FactInternetSales_CustomerKey] ([UnitPrice]);
CREATE STATISTICS [SalesAmount] ON [FactInternetSales_CustomerKey] ([SalesAmount]);
```

```
--Rename the tables
RENAME OBJECT [dbo].[FactInternetSales] TO [FactInternetSales_ProductKey];
RENAME OBJECT [dbo].[FactInternetSales_CustomerKey] TO [FactInternetSales];
```

## <h2 id="section1">Reading and Writing to Synapse</h2>
 
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

```
storageAccount = "name-of-your-storage-account"
containerName = "data"
accessKey = "your-storage-key"
jdbcURI = ""

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", accessKey)
```

 #### Read from the Customer Table
 
 Next, use the Synapse Connector to read data from the Customer Table.
 
 Use the read to define a tempory table that can be queried.
 
 Note: 
 - the connector uses a caching directory on the Azure Blob Container.
 - `forwardSparkAzureStorageCredentials` is set to `true` so that the Synapse instance can access the blob for its MPP read via Polybase

```
 cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir

  tableName = "dbo.DimCustomer"

  customerDF = (spark.read
     .format("com.databricks.spark.sqldw")
     .option("url", jdbcURI)
     .option("tempDir", cacheDir)
     .option("forwardSparkAzureStorageCredentials", "true")
     .option("dbTable", tableName)
     .load())

  customerDF.createOrReplaceTempView("customer_data")
```

 ###  Use SQL queries to count the number of rows in the Customer table and to display table metadata.
```
%sql
select count(*) from customer_data

 %sql
 describe customer_data
 ```
 Note that `CustomerKey` and `CustomerAlternateKey` use a very similar naming convention.

 ```
  %sql
  select CustomerKey, CustomerAlternateKey from customer_data limit 10:
 ``` 
 
 In a situation in which we may be merging many new customers into this table, we can imagine that we may have issues with uniqueness with regard to the `CustomerKey`. Let us redefine `CustomerAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
 
 To do this we will define a UDF and use it to transform the `CustomerAlternateKey` column. Once this is done, we will write the updated Customer Table to a Staging table.
 
 **Note:** It is a best practice to update the Synapse instance via a staging table.
 
```
import uuid

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)
```
 
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

 ```
 customerTempDF.createOrReplaceTempView("customer_temp_data")

 %sql
 select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;
 ```


##  <h2 id="section2">Query data in the lake using Azure Synapse serverless SQL pools</h2>

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
  
## <h2 id="section3">Create metadata objects in Azure Synapse serverless SQL pools</h2>

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
   
 ```
 CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeaderFormat `
   
   WITH (
   
    FORMAT_TYPE = DELIMITEDTEXT, 
    
    FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2   ) 
    
    ); 
    
 GO 
   
 CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (  FORMAT_TYPE = PARQUET );
 ```
   
   --Create an external table on protected data
   
   With the database scoped credential, external data source, and external file format defined, you can create external tables that access data on an Azure storage account that allows access to users with some Azure AD identity or SAS key. You can create external tables the same way you create regular SQL Server external tables. 
   
```USE [mydbname]; 
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
);
```

  --Create an external table on public data
  
  You can create external tables that read data from the files placed on publicly available Azure storage. This setup script will create a public external data source with a Parquet file format definition that is used in the following query:

``` 
CREATE EXTERNAL TABLE Taxi ( 
    vendor_id VARCHAR(100) COLLATE Latin1_General_BIN2,  
    pickup_datetime DATETIME2,  
    dropoff_datetime DATETIME2, 
    passenger_count INT, 
    trip_distance FLOAT, 
    fare_amount FLOAT, 
    tip_amount FLOAT, 
    tolls_amount FLOAT, 
    total_amount FLOAT 
) WITH ( 
        LOCATION = 'puYear=*/puMonth=*/*.parquet', 
        DATA_SOURCE = YellowTaxi, 
        FILE_FORMAT = ParquetFormat 
);
```
   
* Create views in Azure Synapse serverless SQL pools

Views will allow you to reuse queries that you create. Views are also needed if you want to use tools such as Power BI to access the data in conjunction with serverless SQL pools.

  *  Prerequisites
  
  Your first step is to create a database where you will execute the queries. Then initialize the objects by executing the following setup script on that database. This setup script will create the data sources, database scoped credentials, and external file formats that are used in these samples.
  
  --Create a view
  
  You can create views in the same way you create regular SQL Server views. The following query creates a view that reads population.csv file.
  
```
USE [mydbname]; 
GO 
 
DROP VIEW IF EXISTS populationView; 
GO 
 
CREATE VIEW populationView AS 
SELECT *  
FROM OPENROWSET( 
        BULK 'csv/population/population.csv', 
        DATA_SOURCE = 'SqlOnDemandDemo', 
        FORMAT = 'CSV',  
        FIELDTERMINATOR =',',  
        ROWTERMINATOR = '\n' 
    ) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, 
    [population] bigint 
) AS [r];
```

--The view in this example uses the OPENROWSET function that uses an absolute path to the underlying files. If you have EXTERNAL DATA SOURCE with a root URL of your storage, you can use OPENROWSET with DATA_SOURCE and relative file path:

```
CREATE VIEW TaxiView 
AS SELECT *, nyc.filepath(1) AS [year], nyc.filepath(2) AS [month] 
FROM 
    OPENROWSET( 
        BULK 'parquet/taxi/year=*/month=*/*.parquet', 
        DATA_SOURCE = 'sqlondemanddemo', 
        FORMAT='PARQUET' 
    ) AS nyc`
    
    
    USE [mydbname]; 
GO 
 
 --Use view
 
SELECT 
    country_name, population 
FROM populationView 
WHERE 
    [year] = 2019 
ORDER BY 
    [population] DESC;
```    
    
## <h2 id="section4">Use Azure Synapse serverless SQL pools for transforming the data in the lake</h2>

To use CREATE EXTERNAL TABLE AS SELECT statements to transform data and encapsulate the transformation logic in stored procedures.

### Transform data with Azure Synapse serverless SQL pools using CREATE EXTERNAL TABLE AS SELECT statement

* Transform data with Azure Synapse serverless SQL pools using CREATE EXTERNAL TABLE AS SELECT statement

Azure Synapse serverless SQL pools can be used for data transformations. If you are familiar with Transact-SQL syntax, you can craft a SELECT statement that executes the specific transformation you are interested in, and store the results of the SELECT statement in a selected file format. You can use CREATE EXTERNAL TABLE AS SELECT (CETAS) in a dedicated SQL pool or serverless SQL pool to complete the following tasks:

  *  Create an external table.
  
  *  Export, in parallel, the results of a Transact-SQL SELECT statement to the data lake.

  * <a href="https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-external-tables?tabs=hadoop">Use external tables with Synapse SQL</a>
  
  
### Operationalize data transformation with Azure Synapse serverless SQL pools using stored procedures

Once you prepare your data transformation query, you can encapsulate the transformation logic using stored procedures. Stored procedures can accept input parameters and return multiple values in the form of output parameters to the calling program. 

  * <a href="./sqlscript/transformbycetas.sql">Examples</a>
  
### Use Azure Synapse serverless SQL pool stored procedures from Synapse Pipelines

 * You can use Azure Synapse Pipelines to operationalize data transformations using serverless SQL pool by performing the following steps.

   * Open your Azure Synapse Analytics workspace, go to the Data hub

   * Click +, choose Connect to external data and select Azure Data Lake Storage Gen2

   * Fill in Name field with DestinationStorage

   * Pick the Subscription where your storage is

   * Pick the Storage account name (use the same storage you used in CREATE EXTERNAL DATA SOURCE statement above)

   * Create new linked service to destination storage account:

   * Click Test connection to make sure linked service is properly configured

   * Click Create
  
* Create pipeline:

  * Go to the Integrate hub
  
  * Click + and then choose Pipeline to create new pipeline
 
  * In pipeline properties, fill in Name field with MyAggregatingPipeline

* Add Delete activity to delete destination storage folder:

  * From Activities, under the group General, drag the Delete activity to canvas

  * In the Activity General tab, fill in the Name with DeleteFolder

  * In the Source tab click New Dataset, choose Azure Data Lake Storage Gen2 and then click Continue

  * Fill in the Name field with CETASDestination

  * For the Linked service pick DestinationStorage

  * Fill in File path with name of container you used in the CREATE EXTERNAL DATA SOURCE

  * Fill in Directory with LOCATION you used in the CETAS within the CREATE PROCEDURE statement
  
* Add Stored procedure activity that will execute stored procedure:

  * From Activities, under the group General, drag Stored procedure activity to canvas

  * In Activity General tab, fill in Name with AggregateByYearAndState  
  
  * In Activity Settings tab, click New to create new Linked service – here we will create connection to serverless SQL pool

  * In New linked service panel:
  
  * In Activity Settings tab, pick population_by_year_state for Stored Procedure Name
  
* Link Delete and Stored procedure activities to define order of execution of activities:

 * Drag green connector from the Delete activity to Stored procedure activity. Click OK

## <h2 id="section5">Integrate SQL and Apache Spark pools in Azure Synapse Analytics</h2>

<a href="./Apache-Spack/dataengineeringbyspark.scala">Example</a>

The Azure Synapse Analytics environment enables you to use both technologies within one integrated platform. The integrated platform experience allows you to switch between Apache Spark and SQL based data engineering tasks applicable to the expertise you have in-house. As a result, an Apache Spark-orientated data engineer can easily communicate and work with a SQL-based data engineer on the same platform.

* The interoperability between Apache Spark and SQL helps you achieve as follows:

  * Work with SQL and Apache Spark to directly explore and analyze Parquet, CSV, TSV, and JSON files stored in the data lake.

  * Enable fast, scalable loads for data transferring between SQL and Apache Spark databases.

  * Make use of a shared Hive-compatible metadata system that enables you to define tables on files in the data lake such that it can be consumed by either Apache Spark or Hive.

* The Azure Synapse Apache Spark to Synapse SQL connector is designed to efficiently transfer data between serverless Apache Spark pools and dedicated SQL pools in Azure Synapse. At the moment, the Azure Synapse Apache Spark to Synapse SQL connector works on dedicated SQL pools only, it doesn't work with serverless SQL pools.

  * Therefore, a new approach is to use both JDBC and PolyBase. First, the JDBC opens a connection and issues Create External Tables As Select (CETAS) statements and sends filters and projections. The filters and projections are then applied to the data warehouse and exports the data in parallel using PolyBase. Apache Spark reads the data in parallel based on the user-provisioned workspace and the default data lake storage.

  * As a result, you can use the Azure Synapse Apache Spark Pool to Synapse SQL connector to transfer data between a Data Lake store via Apache Spark and dedicated SQL Pools efficiently.

* The Apache Spark and SQL integration that is available within Azure Synapse analytics provides several benefits:

  * You can take advantage of the big data computational power that Apache Spark offers
  * There is flexibility in the use of Apache Spark and SQL languages and frameworks on one platform
  * The integration is seamless and doesn’t require a complex setup
  * SQL and Apache Spark share the same underlying metadata store to transfer data easily
  
* The integration can be helpful in use cases where you perform an ETL process predominately using SQL but need to call on the computation power of Apache Spark to perform a portion of the extract, transform, and load (ETL) process as it is more efficient.

  * Let's say you would like to write data to a SQL pool after you've performed engineering tasks in Apache Spark. You can reference the dedicated SQL pool data as a source for joining with an Apache Spark DataFrame that can contain data from other files. The method makes use of the Azure Synapse Apache Spark to Synapse SQL connector to efficiently transfer data between the Apache Spark and SQL Pools.
  
  * The Azure Synapse Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark. It uses the Azure Data Lake Storage Gen2 and PolyBase in SQL pools to efficiently transfer data between the Spark cluster and the Synapse SQL instance.
  
  * The other thing to keep in mind is that beyond the capabilities mentioned above, the Azure Synapse Studio experience gives you an integrated notebook experience. Within this notebook experience, you can attach a SQL or Apache Spark pool, and develop and execute transformation pipelines using Python, Scala, and native Spark SQL.

### Authenticate in Azure Synapse Analytics

The authentication between the two systems is made seamless in Azure Synapse Analytics. The Token Service connects with Azure Active Directory to obtain the security tokens to be used when accessing the storage account or the data warehouse in the dedicated SQL pool.

For this reason, there's no need to create credentials or specify them in the connector API if Azure AD-Auth is configured at the storage account and the dedicated SQL pool. If not, SQL Authentication can be specified. The only constraint is that this connector only works in Scala.

* There are some prerequisites to authenticate correctly:

  * The account used needs to be a member of the db_exporter role in the database or SQL pool from which you want to transfer data to or from.
  * The account used needs to be a member of the Storage Blob Data Contributor role on the default storage account.
  * If you want to create users, you need to connect to the dedicated SQL pool database from which you want transfer data to or from as shown in the following example: 
  
 ```
 --SQL Authenticated User
  
  CREATE USER Leo FROM LOGIN Leo;

  --Azure Active Directory User
  
  CREATE USER [chuck@contoso.com] FROM EXTERNAL PROVIDER;
  ```

  * When you want to assign the user account to a role, you can use the following script as an example:

  ```
  --SQL Authenticated user
  
  EXEC sp_addrolemember 'db_exporter', 'Leo';

  --Azure Active Directory User

  EXEC sp_addrolemember 'db_exporter',[chuck@contoso.com]
  ```
  
  * Once the authentication is in place, you can transfer data to or from a dedicated SQL pool attached within the workspace.

### Transfer data between SQL and spark pool in Azure Synapse Analytics

By using Azure Active Directory to transfer data to and from an Apache Spark pool and a dedicated SQL pool attached within the workspace you have created for your Azure Synapse Analytics account. If you're using the notebook experience from the Azure Synapse Studio environment linked to your workspace resource, you don’t have to use import statements. Import statements are only required when you don't go through the integrated notebook experience.

* It is important that the Constants and the SqlAnalyticsConnector are set up as shown below:

 `#scala`
 
 `import com.microsoft.spark.sqlanalytics.utils.Constants`
 
 `import org.apache.spark.sql.SqlAnalyticsConnector._`
 
* To read data from a dedicated SQL pool, you should use the Read API. The Read API works for Internal tables (Managed Tables) and External Tables in the dedicated SQL pool.

 The Read API using Azure AD looks as follows:

 `#scala`
 
 `val df = spark.read.sqlanalytics("<DBName>.<Schema>.<TableName>")`

* To write data to a dedicated SQL Pool, you should use the Write API. The Write API creates a table in the dedicated SQL pool. Then, it invokes Polybase to load the data into the table that was created. One thing to keep in mind is that the table can't already exist in the dedicated SQL pool. If that happens, you'll receive an error stating: "There is already an object named..."

 The Write API using Azure Active Directory (Azure AD) looks as follows:
 
 `df.write.sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)`
 
 * The parameters it takes in are:
 -- DBName: the name of the database.
 -- Schema: the schema definition such as dbo.
 -- TableName: the name of the table you want to read data from.
 -- TableType: specification of the type of table, which can have two values.
 -- Constants.INTERNAL - Managed table in dedicated SQL pool
 -- Constants.EXTERNAL - External table in dedicated SQL pool
 
 * To use a SQL pool external table, you need to have an EXTERNAL DATA SOURCE and an EXTERNAL FILE FORMAT that exists on the pool using the following examples:
 
  --For an external table, you need to pre-create the data source and file format in dedicated SQL pool using **SQL** queries:
  
  ```
  CREATE EXTERNAL DATA SOURCE <DataSourceName>
  WITH
   ( LOCATION = 'abfss://...' ,
    TYPE = HADOOP
  ) ;
 CREATE EXTERNAL FILE FORMAT <FileFormatName>
 WITH (  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
 );
 ```
 
* It is not necessary to create an EXTERNAL CREDENTIAL object if you are using Azure AD pass-through authentication from the storage account. The only thing you need to keep in mind is that you need to be a member of the "Storage Blob Data Contributor" role on the storage account. The next step is to use the df.write command within **Scala** with DATA_SOURCE, FILE_FORMAT, and the sqlanalytics command in a similar way to writing data to an internal table.

 The example is shown below:
 
 ```
 df.write.
    option(Constants.DATA_SOURCE, <DataSourceName>).
    option(Constants.FILE_FORMAT, <FileFormatName>).
    sqlanalytics("<DBName>.<Schema>.<TableName>", Constants.EXTERNAL)
 ```   
    
### Authenticate between spark and SQL pool in Azure Synapse Analytics

Another way to authenticate is using SQL Authentication, instead of Azure Active Directory (Azure AD) with the Azure Synapse Apache Spark Pool to Synapse SQL connector. Currently, the Azure Synapse Apache Spark Pool to Synapse SQL connector does not support a token-based authentication to a dedicated SQL pool that is outside of the workspace of Synapse Analytics. In order to establish and transfer data to a dedicated SQL pool that is outside of the workspace without Azure AD, you would have to use SQL Authentication.

* To read data from a dedicated SQL pool outside your workspace without Azure AD, you use the Read API. The Read API works for Internal tables (Managed Tables) and External Tables in the dedicated SQL pool.

The Read API looks as follows when using SQL Authentication:

`val df = spark.read.`

`option(Constants.SERVER, "samplews.database.windows.net").`

`option(Constants.USER, <SQLServer Login UserName>).`

`option(Constants.PASSWORD, <SQLServer Login Password>).`

 `sqlanalytics("<DBName>.<Schema>.<TableName>")`
 
* In order to write data to a dedicated SQL Pool, you use the Write API. The Write API creates the table in the dedicated SQL pool, and then uses Polybase to load the data into the table that was created.

The Write API using SQL Auth looks as follows:

`df.write.`

`option(Constants.SERVER, "samplews.database.windows.net").`

`option(Constants.USER, <SQLServer Login UserName>).`

`option(Constants.PASSWORD, <SQLServer Login Password>).`

`sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)`

* Integrate SQL and Spark Pools in Azure Synapse Analytics by <a href="./sqlscript/synapse-integrate.sql"> examples</a>

 * Transferring data between Apache Spark pools and SQL pools can be done using JavaDataBaseConnectivity (JDBC). However, given two distributed systems such as Apache Spark and SQL pools, JDBC tends to be a bottleneck with serial data transfer.

 * The Azure Synapse Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark. It uses the Azure Data Lake Storage Gen2 and PolyBase in SQL pools to efficiently transfer data between the Spark cluster and the Synapse SQL instance.
 
 <a href="./Apache-Spack/dataengineeringbyspark.scala">Example</a>
 
### Externalize the use of Spark Pools within Azure Synapse workspace

* You can allow other users to use the Azure Synapse Apache Spark to Synapse SQL connector in your Azure Synapse workspace.

  * First of all, it is necessary to be a Storage Blob Data Owner in relation to the Azure Data Lake Gen 2 storage account that is connected to your workspace. The reason why the user account has to be a member of that role is so that you can alter missing permissions for others. In addition to the above, the user needs to have access to the Azure Synapse workspace. Finally, in order to allow other users to use the connector, it's imperative that the user has permissions to run the notebooks.

* You can configure the ACLs for all folders from "synapse" and downward from Azure portal. If you want to configure the ACLs from the root "/" folder, there are some extra instructions you should follow:

  * Connect to the storage account that is connected to the Azure Synapse workspace. You can use Azure Storage Explorer to do so.
  * Select your Account and give the Azure Data Lake Storage Gen 2 URL, and the default file system for the Azure Synapse workspace.
  * If you see the storage account listed, right-click on the listing workspace and make sure you select "Manage Access".
  * Add the user to the root "/" folder with "Execute" access permission and select "Ok".

  <a href="./Apache-Spack/dataengineeringbyspark.scala">Example</a>
  
  
### Transfer data outside the Synapse workspace using SQL Authentication

You can transfer data to and from a dedicated SQL pool using a Pyspark Connector, which currently works with Scala.

Let's say that you have created or loaded a DataFrame called "pyspark_df", and then assume that you want to write that DataFrame into the data warehouse. How would you go about that task?

* The first thing to do is to create a temporary table in a DataFrame in `PySpark` using the createOrReplaceTempView method

`pyspark_df.createOrReplaceTempView("pysparkdftemptable")`

* The parameter that is passed through is the temporary table name, which in this case is called: "pysparkdftemptable". We are still using the pyspark_df DataFrame as you can see in the beginning of the statement. Next, you would have to run a Scala cell in the PySpark notebook using magics (since we're using different languages, and it will only work in Scala):

`%%spark`

`val scala_df = spark.sqlContext.sql ("select * from pysparkdftemptable")`

`scala_df.write.sqlanalytics("sqlpool.dbo.PySparkTable", Constants.INTERNAL)`

* By using "val scala_df", we create a fixed value for the scala_dataframe, and then use the statement "select * from pysparkdftemptable", that returns all the data that we created in the temporary table in the previous step, and storing it in a table named sqlpool.dbo.PySparkTable

**Should you wish to read data using the PySpark connector, keep in mind that you read the data using scala first, then write it into a temporary table. Finally you use the Spark SQL in PySpark to query the temporary table into a DataFrame.**


## <h2 id="section6">Serve the data from Azure Synapse serverless SQL pool</h2>

To serve and make use of the data that is queried or transformed by Azure Synapse serverless SQL pool.

### Use Synapse Studio to analyze and visualize data via Azure Synapse serverless SQL pool

* Azure Synapse serverless SQL pool provides a look and feel of SQL Server to the clients connecting to it. This enables a huge number of clients to interact with serverless SQL pool to query the data in the lake.

* Azure Synapse Studio is the web-native experience that ties everything together for all users, providing one location to do every task you need to build a complete solution.

* To open Synapse Studio, navigate to the Azure Synapse workspace resource in the Azure portal and click on the Open link available in the overview page:

### Use Azure Synapse serverless SQL pools as a data source in Synapse Pipelines

* Azure Synapse Pipelines are a cloud ETL service for scale-out serverless data integration and data transformation. 
* It offers a code-free UI for intuitive authoring and single-pane-of-glass monitoring and management. 
* You can use Azure Synapse Pipeline to orchestrate data integration jobs and serverless SQL pool can be used as part of these jobs. 
* To use serverless SQL pool in Azure Synapse Pipeline, you need to ensure that the built-in serverless SQL pool is properly linked to the Synapse Pipeline. 
* Select the Manage hub in the Azure Synapse Studio menu on the left side, than select Linked services.
* Now navigate to the Integrate hub in the menu on the left side, click on + button and select Pipeline.

### Use Power BI to visualize the data from Azure Synapse serverless SQL pool

* Power BI can be linked to the Azure Synapse workspace. 
* To link your Power BI instance select the Manage hub in the menu on the left, then select the Linked Services item. 
* If your Power BI instance is not already in the linked services list, click on the + New button in command menu. 
* In the page that will appear on the right select Power BI, then find the Power BI instance you want to connect to the workspace. 
* Once that is done, your Power BI instance is linked to the Azure Synapse workspace.

* To start creating Power BI reports using serverless SQL pool, select the Develop hub in the menu on the left, expand the Power BI item, select your Power BI instance, and select Power BI data sets. 
* In the command bar select + New Power BI dataset. A page on the right will appear with databases available in the Azure Synapse workspace. 
* Click on Download .pbids file next to the database you previously created that belongs to the built-in serverless SQL pool.


### Issue queries programmatically on Azure Synapse serverless SQL pool

  <a href="./sqlscript/queriesbycs.cs">Please See Examples!</a>


## <h2 id="section7">Work with Data Warehouses using Azure Synapse Analytics by developer features</h2>

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

## <h2 id="section8">Use data loading best practices in Azure Synapse Analytics</h2> 

### Load methods into Azure Synapse Analytics
1. **Data loads directly from Azure storage with transact-sql and the copy statement**
2. **Perform data loads using Azure synapse pipeline data flows.**
3. **Use polybase by defining external tables**
  
### Set-up dedicated data load accounts
1. **The first step is to connect to master and create a login.**
```
-- Connect to master
CREATE LOGIN loader WITH PASSWORD = 'a123STRONGpassword!';
```
2. **Next, connect to the dedicated SQL pool and create a user.**
```
-- Connect to the SQL pool
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
);
```

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
  
  ```
  CREATE TABLE [wwi_staging].DailySalesCounts
    (
        [Date] [int]  NOT NULL,
        [NorthAmerica] [int]  NOT NULL,
        [SouthAmerica] [int]  NOT NULL,
        [Europe] [int]  NOT NULL,
        [Africa] [int]  NOT NULL,
        [Asia] [int]  NOT NULL
    )
   GO
   ```

**Note:** Replace <PrimaryStorage> with the workspace default storage account name.
 
```
COPY INTO wwi_staging.DailySalesCounts
FROM 'https://YOURACCOUNT.dfs.core.windows.net/wwi-02/campaign-analytics/dailycounts.txt'
WITH (
    FILE_TYPE = 'CSV',
    FIELDTERMINATOR='.',
    ROWTERMINATOR=','
)
GO
```
 
 * Attempt to load using PolyBase to create a new external file format, external table, and load data using PolyBase:
   
 ```
 CREATE EXTERNAL FILE FORMAT csv_dailysales
     WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
        FIELD_TERMINATOR = '.',
        DATE_FORMAT = '',
        USE_TYPE_DEFAULT = False
    )
  );
 GO
 ```

```
CREATE EXTERNAL TABLE [wwi_external].DailySalesCounts
    (
        [Date] [int]  NOT NULL,
        [NorthAmerica] [int]  NOT NULL,
        [SouthAmerica] [int]  NOT NULL,
        [Europe] [int]  NOT NULL,
        [Africa] [int]  NOT NULL,
        [Asia] [int]  NOT NULL
    )
WITH
    (
        LOCATION = '/campaign-analytics/dailycounts.txt',
        DATA_SOURCE = ABSS,
        FILE_FORMAT = csv_dailysales
    )
GO
INSERT INTO [wwi_staging].[DailySalesCounts]
SELECT *`<br>
FROM [wwi_external].[DailySalesCounts]
```
 
 **<a href="analyze-optimize-performamce.md">Analyze and optimize data warehouse storage in Azure Synapse Analytics</a>**

 
 ## <h2 id="section9">Manage and monitor data warehouse activities in Azure Synapse Analytics</h2>
 
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

## <h2 id="section2-1">Analyze data with Apache Spark in Azure Synapse Analytics</h2>
 
### Analyze data with Spark

One of the benefits of using Spark is that you can write and run code in various programming languages, enabling you to use the programming skills you already have and to use the most appropriate language for a given task. The default language in a new Azure Synapse Analytics Spark notebook is PySpark - a Spark-optimized version of Python, which is commonly used by data scientists and analysts due to its strong support for data manipulation and visualization. Additionally, you can use languages such as Scala (a Java-derived language that can be used interactively) and SQL (a variant of the commonly used SQL language included in the Spark SQL library to work with relational data structures). Software engineers can also create compiled solutions that run on Spark using frameworks such as Java and Microsoft .NET. 
 
#### Exploring data with dataframes
 
Natively, Spark uses a data structure called a resilient distributed dataset (RDD); but while you can write code that works directly with RDDs, the most commonly used data structure for working with structured data in Spark is the dataframe, which is provided as part of the Spark SQL library. Dataframes in Spark are similar to those in the ubiquitous Pandas Python library, but optimized to work in Spark's distributed processing environment.

 **Note**

 In addition to the Dataframe API, Spark SQL provides a strongly-typed Dataset API that is supported in Java and Scala. We'll focus on the Dataframe API in this module. 
 
**Loading data into a dataframe**
 
Let's explore a hypothetical example to see how you can use a dataframe to work with data. Suppose you have the following data in a comma-delimited text file named products.csv in the primary storage account for an Azure Synapse Analytics workspace:
``` 
ProductID,ProductName,Category,ListPrice
771,"Mountain-100 Silver, 38",Mountain Bikes,3399.9900
772,"Mountain-100 Silver, 42",Mountain Bikes,3399.9900
773,"Mountain-100 Silver, 44",Mountain Bikes,3399.9900
...
```
In a Spark notebook, you could use the following PySpark code to load the data into a dataframe and display the first 10 rows:
```
 %%pyspark
df = spark.read.load('abfss://container@store.dfs.core.windows.net/products.csv',
    format='csv',
    header=True
)
display(df.limit(10))
```
The %%pyspark line at the beginning is called a magic, and tells Spark that the language used in this cell is PySpark. You can select the language you want to use as a default in the toolbar of the Notebook interface, and then use a magic to override that choice for a specific cell. For example, here's the equivalent Scala code for the products data example:
```
 %%spark
val df = spark.read.format("csv").option("header", "true").load("abfss://container@store.dfs.core.windows.net/products.csv")
display(df.limit(10))
```
**Specifying a dataframe schema**
 
In the previous example, the first row of the CSV file contained the column names, and Spark was able to infer the data type of each column from the data it contains. You can also specify an explicit schema for the data, which is useful when the column names aren't included in the data file, like this CSV example: 
```
 771,"Mountain-100 Silver, 38",Mountain Bikes,3399.9900
772,"Mountain-100 Silver, 42",Mountain Bikes,3399.9900
773,"Mountain-100 Silver, 44",Mountain Bikes,3399.9900
...
```
 The following PySpark example shows how to specify a schema for the dataframe to be loaded from a file named product-data.csv in this format:
```
 from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('abfss://container@store.dfs.core.windows.net/product-data.csv',
    format='csv',
    schema=productSchema,
    header=False)
display(df.limit(10))
```
**Filtering and grouping dataframes**
 
You can use the methods of the Dataframe class to filter, sort, group, and otherwise manipulate the data it contains. For example, the following code example uses the select method to retrieve the ProductName and ListPrice columns from the df dataframe containing product data in the previous example:  `pricelist_df = df.select("ProductID", "ListPrice")`
 
In common with most data manipulation methods, select returns a new dataframe object.

 **Tip**

Selecting a subset of columns from a dataframe is a common operation, which can also be achieved by using the following shorter syntax:

`pricelist_df = df["ProductID", "ListPrice"]`

You can "chain" methods together to perform a series of manipulations that results in a transformed dataframe. For example, this example code chains the select and where methods to create a new dataframe containing the ProductName and ListPrice columns for products with a category of Mountain Bikes or Road Bikes: 
 ```
 bikes_df = df.select("ProductName", "ListPrice").where((df["Category"]=="Mountain Bikes") | (df["Category"]=="Road Bikes"))
display(bikes_df)
 ```
 To group and aggregate data, you can use the groupBy method and aggregate functions. For example, the following PySpark code counts the number of products for each category:
 ```
 counts_df = df.select("ProductID", "Category").groupBy("Category").count()
display(counts_df)
 ```
#### Using SQL expressions in Spark
 
The Dataframe API is part of a Spark library named Spark SQL, which enables data analysts to use SQL expressions to query and manipulate data.

**Creating database objects in the Spark catalog**
 
The Spark catalog is a metastore for relational data objects such as views and tables. The Spark runtime can use the catalog to seamlessly integrate code written in any Spark-supported language with SQL expressions that may be more natural to some data analysts or developers.

One of the simplest ways to make data in a dataframe available for querying in the Spark catalog is to create a temporary view, as shown in the following code example: `df.createOrReplaceTempView("products")`

 A view is temporary, meaning that it's automatically deleted at the end of the current session. You can also create tables that are persisted in the catalog to define a database that can be queried using Spark SQL.

 **Note**

We won't explore Spark catalog tables in depth in this module, but it's worth taking the time to highlight a few key points:

* You can create an empty table by using the spark.catalog.createTable method. Tables are metadata structures that store their underlying data in the storage location associated with the catalog. Deleting a table also deletes its underlying data.
* You can save a dataframe as a table by using its saveAsTable method.
* You can create an external table by using the spark.catalog.createExternalTable method. External tables define metadata in the catalog but get their underlying data from an external storage location; typically a folder in a data lake. Deleting an external table does not delete the underlying data.
 
**Using the Spark SQL API to query data**
 
You can use the Spark SQL API in code written in any language to query data in the catalog. For example, the following PySpark code uses a SQL query to return data from the products view as a dataframe. 
 ```
 bikes_df = spark.sql("SELECT ProductID, ProductName, ListPrice \
                      FROM products \
                      WHERE Category IN ('Mountain Bikes', 'Road Bikes')")
display(bikes_df)
```
 
**Using SQL code**
 
The previous example demonstrated how to use the Spark SQL API to embed SQL expressions in Spark code. In a notebook, you can also use the %%sql magic to run SQL code that queries objects in the catalog, like this: 
 ```
 %%sql

SELECT Category, COUNT(ProductID) AS ProductCount
FROM products
GROUP BY Category
ORDER BY Category
 ```
 
### Visualize data with Spark

One of the most intuitive ways to analyze the results of data queries is to visualize them as charts. Notebooks in Azure Synapse Analytics provide some basic charting capabilities in the user interface, and when that functionality doesn't provide what you need, you can use one of the many Python graphics libraries to create and display data visualizations in the notebook.

#### Using built-in notebook charts
 
When you display a dataframe or run a SQL query in a Spark notebook in Azure Synapse Analytics, the results are displayed under the code cell. By default, results are rendered as a table, but you can also change the results view to a chart and use the chart properties to customize how the chart visualizes the data, as shown here: 

The built-in charting functionality in notebooks is useful when you're working with results of a query that don't include any existing groupings or aggregations, and you want to quickly summarize the data visually. When you want to have more control over how the data is formatted, or to display values that you have already aggregated in a query, you should consider using a graphics package to create your own visualizations.

**Using graphics packages in code**
 
There are many graphics packages that you can use to create data visualizations in code. In particular, Python supports a large selection of packages; most of them built on the base Matplotlib library. The output from a graphics library can be rendered in a notebook, making it easy to combine code to ingest and manipulate data with inline data visualizations and markdown cells to provide commentary.

For example, you could use the following PySpark code to aggregate data from the hypothetical products data explored previously in this module, and use Matplotlib to create a chart from the aggregated data.
``` 
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
data = spark.sql("SELECT Category, COUNT(ProductID) AS ProductCount \
                  FROM products \
                  GROUP BY Category \
                  ORDER BY Category").toPandas()

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by category
plt.bar(x=data['Category'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by Category')
plt.xlabel('Category')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
``` 

The Matplotlib library requires data to be in a Pandas dataframe rather than a Spark dataframe, so the toPandas method is used to convert it. The code then creates a figure with a specified size and plots a bar chart with some custom property configuration before showing the resulting plot.
 
 You can use the Matplotlib library to create many kinds of chart; or if preferred, you can use other libraries such as Seaborn to create highly customized charts.
 
## <h2 id="section2-2">Ingest data with Apache Spark notebooks in Azure Synapse Analytics</h2>

### Understand the use-cases for spark notebooks

There are various use cases that make using notebooks compelling within Azure Synapse Analytics 

 #### To perform exploratory data analysis using a familiar paradigm
 
Many data engineers and data scientist work with Apache Spark in various incarnations, be it Microsoft Azure HDInsight, Azure Databricks, or even open-source Apache Spark. The notebooks that are available within Azure Synapse Analytics contain many of the features that these professionals are used to when working with notebooks to explore the data within their organizations. It enables data engineers and scientists to quickly launch a notebook to explore data without the need to learn a new tool, while taking advantage of the inherent integration that the notebooks in Azure Synapse Analytics has with other aspects of the product.

#### To integrate notebooks as part of a broader data transformation process
 
Running complex or large transformation on an Apache Spark cluster is at times far more efficient than trying to perform the transformation using traditional relational Transact-SQL methods. You can use notebooks to perform transformations using Apache Spark, and then integrate this transformation into a broader extract, transform, and load (ETL) process by integrating the notebook into an ETL tool such as Azure Data Factory, or Azure Synapse pipelines.

#### You wish to perform advanced analytics using notebooks with Azure Machine Learning Services
 
You can create Apache Spark tables in a notebook to connect to a linked service for Azure Machine Learning Services to perform various advanced analytical tasks from classical machine learning to deep learning, both supervised and unsupervised. Whether you prefer to write Python or R code with the SDK or work with no-code/low-code options in Azure Machine Learning Studio, you can build and train machine learning and deep-learning models using the notebooks, and track their activity in an Azure Machine Learning Workspace. 

### Load data in spark notebooks

In order to ingest data into a notebook, there are several options. Currently it is possible to load data from an Azure Storage Account, and an Azure Synapse Analytics dedicated SQL pool.

Some examples for reading data in a notebook are:

* Read a CSV from Azure Data Lake Store Gen2 as an Apache Spark DataFrame
* Read a CSV from Azure Storage Account as an Apache Spark DataFrame
* Read data from the primary storage account
 
**Example 1: Read a CSV file from an Azure Data Lake Store Gen2 store as an Apache Spark DataFrame.**
 
The following code is used to read a CSV file from an Azure Data Lake Store Gen2 store as an Apache Spark DataFrame.

```
from pyspark.sql import SparkSession
from pyspark.sql.types import *
account_name = "Your account name"
container_name = "Your container name"
relative_path = "Your path"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net" %account_name, "SharedKey")
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" %account_name ,"Your ADLS Gen2 Primary Key")

df1 = spark.read.option('header', 'true') \
                .option('delimiter', ',') \
                .csv(adls_path + '/Testfile.csv')
```
 
There are parameter name values that you need to replace in the above code to ensure that it works, including:

* account_name

Replace "Your account name" with the storage account name you wish to use

* container_name

Replace "Your container name" with the storage container you wish to use

* relative_path

Replace "Your path" with the relative path of where the file is stored

* adls_path

The adls_path is defined by passing through the above parameters.

**Example 2: Read a CSV file from Azure Storage Account as a Spark DataFrame.**
 
The following code is used to read a CSV file from Azure Storage Account as an Apache Spark DataFrame.

```
from pyspark.sql import SparkSession
from pyspark.sql.types import *

blob_account_name = "Your blob account name"
blob_container_name = "Your blob container name"
blob_relative_path = "Your blob relative path"
blob_sas_token = "Your blob sas token"

wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)

df = spark.read.option("header", "true") \
            .option("delimiter","|") \
            .schema(schema) \
            .csv(wasbs_path)
```
There are parameter name values that you need to replace in the above code to ensure that it works, including:

* blob_account_name

Replace "Your blob account name" with the name of your blob account.

* blob_container_name

Replace "Your blob container" with the name of the blob container the file is in.

* blob_relative_path

Replace "Your blob relative path" with the name of the relative path pointing to the csv you want to read.

* blob_sas_token

Replace "Your blob sas token" with the blob SAS key.

**Example 3: Read data from the primary storage account**
 
The third possibility is to read data from the primary storage account, using the Data tab in the Azure Synapse Studio environment. If you right-click on a file and select New notebook, you will see a new notebook with the data generated.

New notebook to pass Data and Load into Notebook

**Note**
 
 If you would like to load data to or from a table into a Spark DataFrame, you can use the Azure Synapse Apache Spark pool to Synapse SQL connector. The Azure Synapse Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark, and it uses Azure Data Lake Storage Gen2 and PolyBase in dedicated SQL pools to efficiently transfer data between the Spark cluster and the Azure Synapse dedicated SQL pool instance.
 ```
 %%spark
spark.sql("CREATE DATABASE IF NOT EXISTS nyctaxi")
val df = spark.read.sqlanalytics("SQLPOOL1.dbo.Trip") 
df.write.mode("overwrite").saveAsTable("nyctaxi.trip")
 ```
In this code example, the spark.sql method is used to create a database named nyctaxi. A DataFrame named df reads data from a table named Trip in the SQLPOOL1 dedicated SQL pool instance. Finally, the DataFrame df writes data into it and used the saveAsTable method to save it as nyctaxi.trip.

As you can see, there are various ways to load data into an Apache Spark DataFrame depending on the source.
 
#### Flatten nested structures and explode arrays with Apache Spark

A common use case for using Apache Spark pools in Azure Synapse Analytics is for transforming complex data structures using DataFrames. It can help for the following reasons:

* Complex data types are increasingly common and represent a challenge for data engineers because analyzing nested schema and data arrays often include time-consuming and complex SQL queries.
* It can be difficult to rename or cast the nested column data type.
* Performance issues arise when working with deeply nested objects. Data Engineers need to understand how to efficiently process complex data types and make them easily accessible to everyone. In the following example, Apache Spark for Azure Synapse is used to read and transform objects into a flat structure through data frames.
* Apache Synapse SQL serverless is used to query such objects directly and return those results as a regular table.
* With Azure Synapse Apache Spark pools, it's easy to transform nested structures into columns and array elements into multiple rows.
 
In the example, the following steps show the techniques involved to deal with complex data types by creating multiple DataFrames to achieve the desired result.

**Flatten Nested Structures Steps** 

Step 1: Define a function for flattening
We create a function that will flatten the nested schema.

Step 2: Flatten nested schema
We will define the function you create to flatten the nested schema from one DataFrame into a new DataFrame.

Step 3: Explode Arrays
Here you will transform the data array from the DataFrame created in step 2 into a new DataFrame.

Step 4: Flatten child nested Schema
Finally, you use the transformed DataFrame created in step 3 and load the cleansed data into a destination DataFrame to complete the work. 
 
 
## <h2 id="section2-3">Transform data with DataFrames in Apache Spark Pools in Azure Synapse Analytics</h2>

## <h2 id="section5">Integrate SQL and Apache Spark pools in Azure Synapse Analytics</h2>

## <h2 id="section2-5">Monitor and manage data engineering workloads with Apache Spark in Azure Synapse Analytics</h2>

### Optimize Apache Spark jobs in Azure Synapse Analytics

Once you have checked the Monitor tab within the Azure Synapse Studio environment, and decide that you should improve the performance of an Apache Spark pool run, you have several areas to consider, including:

* Choosing the data abstraction
* Use the optimal data format
* Use the cache option
* Check the memory efficiency
* Use Bucketing
* Optimize Joins and Shuffles if appropriate
* Optimize Job Execution

To optimize the Apache Spark Jobs in Azure Synapse Analytics, you need to consider the cluster configuration for the workload you're running on that cluster. You might run into challenges such as memory pressure (if not configured appropriately by choosing the wrong size of executors), long running operations, and tasks that might result in cartesian operations.

If you want to speed up the jobs, you'd have to configure the appropriate caching for that task, and check joins and shuffles in relation to data skew. Therefore, it is imperative that you monitor and review Apache Spark Job executions that are long running or resource consuming. Some recommendations for you to optimize the Apache Spark Job include:

#### Choosing the data abstraction
 
Some of the earlier Apache Spark versions use Resilient Distributed DataSets (RDDs) to abstract the data. Apache Spark 1.3 and 1.6 introduced the use of DataFrames and DataSets. The following relative merits might help you to optimize in relation to your data abstraction:

**DataFrames**
 
Using DataFrames would be a great place to start. DataFrames provide query optimization through Catalyst. It also includes a whole-stage code generation with direct memory access. When you want to have the best developer-friendly experience, it might be better to use DataSets, since there are no compile-time checks or domain object programming.

DataSets are good in complex ETL pipeline optimization where the performance effect is acceptable. Just be cautious when using DataSets in aggregations, since it might affect the performance. It will provide query optimization through Catalyst and is developer-friendly by providing object programming and compile-time checks.

**RDDs**
 
It is not necessary to use RDDs unless you want or need to build a new custom RDD. However, there is no query optimization through Catalyst and no whole-stage code generation and would still have a high garbage collection (GC) overhead. The only way to use RDDs is with Apache Spark 1.x legacy APIs.

Use the optimal data format
Apache Spark supports many data formats. The formats that you can use are csv, json, xml, parquet etc. It can also be extended by other formats with external data sources. A useful tip is to use Parquet with snappy compression (which also happen to be the default in Apache Spark 2.x.) as it stores data in a columnar format, is compressed and highly optimized in Apache Spark, and is splittable to decompress.

**Use the cache option**
 
When it comes to the caching, there is a native built-in Apache Spark caching mechanism. It can be used through different methods like: .persist(), .cache(), and CACHE TABLE. When using small datasets, it might be effective.

In ETL pipelines where caching of intermediate results is necessary it might come in handy too. Just keep in mind that when you need to do partitioning, the Apache Spark native caching mechanism might have some downsides because a cached table won't keep the partitioning data.

**Check the memory efficiency**
 
It is also imperative to understand how to use the memory efficiently. Apache Spark operates by placing data in memory. Therefore, managing memory resources is an aspect of optimizing Apache Spark jobs executions.

One way to manage memory resources might be to check smaller data partitions and check data size, types, and distributions when you formulate a partitioning strategy. Another way to optimize is to consider Kryo data serialization: Kryo data serialization, versus the default Java serialization. Always bear in mind to keep monitoring and tuning the Apache Spark configuration settings.

**Use Bucketing**
 
Bucketing is almost the same as data partitioning. The way it differs is that a bucket holds a set of column values instead of one. It might work well when you partition on large (millions or more) values like product identifiers. A bucket is determined by hashing the bucket key of a row. Bucketed tables are optimized because it is a metadata operation on how the data is bucketed and sorted.

Some advanced bucketing features are:

* Query optimization based on bucketing meta-information.
* Optimized aggregations.
* Optimized joins.

 However, bucketing doesn't exclude partitioning. They go hand in hand, and you can use partitioning and bucketing at the same time.

**Optimize joins and shuffles**
 
When you have a slower performance on join or shuffle jobs, it can be caused by data skew. Data skew is data that is stored asymmetrically on your system. An example might be that a job typically only takes 20 seconds, however running the same job where data is joined and shuffled can take hours.

To fix that data skew, you can salt the entire key, or use an isolated salt for only some subset of keys. Another option to investigate might be the introduction of a bucket column or pre-aggregated data in buckets.

However, there's more to causing slow performance just by joins alone, since it might be the join type that is causing the slow performance.

Apache Spark uses the SortMerge join type. This type of join is best suited for large data sets but is otherwise computationally expensive because it must first sort the left and right sides of data before merging them. Therefore, a Broadcast join might be better suited for smaller data sets, or where one side of the join is much smaller than the other side.

You can change the join type in your configuration by setting `spark.sql.autoBroadcastJoinThreshold`, or you can set a join hint using the DataFrame APIs (`dataframe.join(broadcast(df2))`) as shown in the following code.

Scala
```
// Option 1
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)

// Option 2
val df1 = spark.table("FactTableA")
val df2 = spark.table("dimMP")
df1.join(broadcast(df2), Seq("PK")).
    createOrReplaceTempView("V_JOIN")

sql("SELECT col1, col2 FROM V_JOIN")
```
 
If you did decide to use bucketed tables, you will have a third join type, the Merge join. A correctly pre-partitioned and pre-sorted dataset will skip the expensive sort phase from a SortMerge join. Another thing to keep in mind is that the order of the different types of joins does matter, especially in complex queries. Therefore, it's advised to start with the most selective joins. In addition, try to move joins that increase the number of rows after aggregations when possible.

#### Optimize Job Execution
 
Looking at the sizing of executors to increase performance in your Apache Spark job, you could consider the Java garbage collection (GC) overhead and use the following factors to reduce executor size:

* Reduce heap size below 32 GB to keep GC overhead < 10%.
* Reduce the number of cores to keep GC overhead < 10%.

Or consider the following factors to increase executor size:

* Reduce communication overhead between executors.
* Reduce the number of open connections between executors (N2) on larger clusters (>100 executors).
* Increase heap size to accommodate for memory-intensive tasks.
* Reduce per-executor memory overhead.
* Increase utilization and concurrency by oversubscribing CPU.

As a rule of thumb, when selecting the executor size:

* Start with 30 GB per executor and distribute available machine cores.
* Increase the number of executor cores for larger clusters (> 100 executors).
* Modify size based both on trial runs and on the preceding factors such as GC overhead.

When running concurrent queries, consider as follows:

* Start with 30 GB per executor and all machine cores.
* Create multiple parallel Apache Spark applications by oversubscribing CPU (around 30% latency improvement).
* Distribute queries across parallel applications.
* Modify size based both on trial runs and on the preceding factors such as GC overhead.

 As stated before, it's important to keep monitoring the performance, especially outliers, using the timeline view, SQL graph, job statistics, and so on. It might be a case where one of the executors is slower than the other, which most frequently happens on large clusters (30+ nodes). What you then might consider is to divide the work into more tasks so that the scheduler can compensate for the slower tasks.

If there is an optimization necessary in relation to the optimization of a job execution, make sure you keep in mind the caching (an example might be using the data twice, but cache it). If you broadcast variables on all the executors you set up, due to the variables only being serialized once, you'll have faster lookups.

In another case you might use the thread pool that runs on the driver, which could result in faster operations for many tasks. 
 
###Automate scaling of Apache Spark pools in Azure Synapse Analytics

Within an Apache Spark Pool, it is possible to configure a fixed size when you disable autoscaling. When you enable autoscale, you can set a minimum and maximum number of nodes in order to control the scale that you'd like.

Once you have enabled autoscale, Azure Synapse Analytics will monitor the resources of the load, and it will scale the number of nodes up or down. There will be continuous monitoring depending on CPU usage, pending memory, free CPU, free memory, and the used memory per node when it comes to the metrics involved to make a decision to scale up or down. It checks these metrics every 30 seconds and makes scaling decisions based on the values. There's no extra charge for this feature. 
 
When autoscale scales up, it will calculate the number of new nodes that would be needed in order to meet the CPU and memory requirements. Next, it will issue the scale-up requests and add the number of nodes required to do the job.

In case autoscale performs the action of scaling down, the decision is based on the number of executors and the application primaries per node, and the CPU and memory requirements. The autoscale functionality will then issue the request to remove some nodes.The autoscale functionality will also check which nodes are candidates for removal based on the current job execution. The scale down operation first decommissions the nodes, and then removes them from the cluster.

If you'd like to get started with the autoscale functionality, you'd have to follow the next steps:

**Create a serverless Apache Spark pool with Autoscaling**

To enable the Autoscale feature, complete the following steps as part of the normal Apache Spark pool creation process:

1. On the Basics tab, select the Enable autoscale checkbox.

2. Enter the desired values for the following properties:

   * Min number of nodes.
   * Max number of nodes.
 
The initial number of nodes will be the minimum. This value defines the initial size of the instance when it's created. The minimum number of nodes can't be fewer than three.

When considering the best practices to use for the autoscale feature, consider latency as part of the scale up or down operations. It could take 1 to 5 minutes in order for the scaling operations (whether that's scaling up or down) to complete. Also, when you scale down, the nodes will first be put in a decommissioned state such that there won't be new executors launching on the node. The jobs that are still running will continue to run and finish, but the pending jobs will be in a waiting state to be scheduled as normal but with fewer nodes.

 
