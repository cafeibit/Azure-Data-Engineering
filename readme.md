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

  
# Use data loading best practices in Azure Synapse Analytics 
  
  
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
  
 Let's find now the distribution of per-customer transaction item counts. Run the following query: 
  
### Use a more advanced approach to understand table space usage
1. **Run the following script to create the `vTableSizes` view:**        
2. **Run the following script to view the details about the structure of the tables in the wwi_perf schema:**

### View column store storage details
1. **Run the following query to create the `vColumnStoreRowGroupStats`:**      
2. **Explore the statistics of the columnstore for the Sale_Partition01 table using the following query:**
3. **Explore the results of the query:**
4. **Explore the statistics of the columnstore for the Sale_Hash_Ordered table using the same query:**
5. **Explore the results of the query:**
  
### Compare storage requirements between optimal and sub-optimal column data types
 
### Improve the execution plan of a query with a materialized view
  
### Understand rules for minimally logged operations & Optimize a delete operation
  
                        
