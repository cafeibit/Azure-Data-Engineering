# Azure Data Factory for Data Engineering by Examples

## Examples from MS Learn

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-powershell">Incrementally load data from Azure SQL Database to Azure Blob storage using PowerShell</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/tutorial-data-flow-write-to-lake">Best practices for writing to files to data lake with data flows</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/tutorial-control-flow-portal">Branching and chaining activities in an Azure Data Factory pipeline using the Azure portal</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions">Expressions and functions in Azure Data Factory and Azure Synapse Analytics</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/tutorial-deploy-ssis-packages-azure">Provision the Azure-SSIS integration runtime in Azure Data Factory</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/copy-activity-overview">Copy activity in Azure Data Factory and Azure Synapse Analytics</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/tutorial-push-lineage-to-purview">Push Data Factory lineage data to Microsoft Purview</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/data-flow-transformation-overview">Mapping data flow transformation overview</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/parameterize-linked-services?tabs=data-factory">Parameterize linked services in Azure Data Factory and Azure Synapse Analytics</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/how-to-create-tumbling-window-trigger?tabs=data-factory%2Cazure-powershell">Create a trigger that runs a pipeline on a tumbling window</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/how-to-send-email">Send an email with an Azure Data Factory or Azure Synapse pipeline</a>

* <a href="https://docs.microsoft.com/en-us/azure/data-factory/data-migration-guidance-overview">Use Azure Data Factory to migrate data from your data lake or data warehouse to Azure</a>

## Examples by My Case Study

### 1. To use PolyBase data loading for dedicated SQL pool in Azure Synapse Analytics and then send an email with an Azure Synapse pipeline to report the loaded result.

#### 1.1 Design a PolyBase data loading strategy for dedicated SQL pool in Azure Synapse Analytics

An Extract, Load, and Transform (ELT) process can take advantage of built-in distributed query processing capabilities and eliminate resources needed to transform the data before loading. While SQL pool supports many loading methods including non-Polybase options such as BCP and SQL BulkCopy API, the fastest and most scalable way to load data is through PolyBase. PolyBase is a technology that accesses external data stored in Azure Blob storage or Azure Data Lake Store via the T-SQL language.

**Extract, Load, and Transform (ELT)**

Extract, Load, and Transform (ELT) is a process by which data is extracted from a source system, loaded into a data warehouse, and then transformed.

The basic steps for implementing a PolyBase ELT for dedicated SQL pool are:

1. Extract the source data into text files.

  * PolyBase external file formats

    Getting data out of your source system depends on the storage location. The goal is to move the data into PolyBase supported delimited text files. PolyBase loads data from UTF-8 and UTF-16 encoded delimited text files. PolyBase also loads from the Hadoop file formats RC File, ORC, and Parquet. PolyBase can also load data from Gzip and Snappy compressed files. PolyBase currently does not support extended ASCII, fixed-width format, and nested formats such as WinZip, JSON, and XML. If you're exporting from SQL Server, you can use bcp command-line tool to export the data into delimited text files.

2. Land the data into Azure Blob storage or Azure Data Lake Store.
  
  To land the data in Azure storage, you can move it to Azure Blob storage or Azure Data Lake Store. In either location, the data should be stored in text files. PolyBase can load from either location.
  
  * Tools and services you can use to move data to Azure Storage:

    * Azure ExpressRoute service enhances network throughput, performance, and predictability. ExpressRoute is a service that routes your data through a dedicated private connection to Azure. ExpressRoute connections do not route data through the public internet. The connections offer more reliability, faster speeds, lower latencies, and higher security than typical connections over the public internet.
    * AzCopy utility moves data to Azure Storage over the public internet. This works if your data sizes are less than 10 TB. To perform loads on a regular basis with AzCopy, test the network speed to see if it is acceptable.
    * Azure Data Factory (ADF) has a gateway that you can install on your local server. Then you can create a pipeline to move data from your local server up to Azure Storage. To use Data Factory with dedicated SQL pool, see Load data into dedicated SQL pool.

3. Prepare the data for loading.

  You might need to prepare and clean the data in your storage account before loading it into dedicated SQL pool. Data preparation can be performed while your data is in the source, as you export the data to text files, or after the data is in Azure Storage. It is easiest to work with the data as early in the process as possible.
  
  * Define external tables

  Before you can load data, you need to define external tables in your data warehouse. PolyBase uses external tables to define and access the data in Azure Storage. An external table is similar to a database view. The external table contains the table schema and points to data that is stored outside the data warehouse.

  Defining external tables involves specifying the data source, the format of the text files, and the table definitions. What follows are the T-SQL syntax topics that you'll need:

     * CREATE EXTERNAL DATA SOURCE
     * CREATE EXTERNAL FILE FORMAT
     * CREATE EXTERNAL TABLE
      
4. Load the data into dedicated SQL pool staging tables using PolyBase.

  It is best practice to load data into a staging table. Staging tables allow you to handle errors without interfering with the production tables. A staging table also gives you the opportunity to use SQL pool built-in distributed query processing capabilities for data transformations before inserting the data into production tables.

**Options for loading with PolyBase**

To load data with PolyBase, you can use any of these loading options:

* PolyBase with T-SQL works well when your data is in Azure Blob storage or Azure Data Lake Store. It gives you the most control over the loading process, but also requires you to define external data objects. The other methods define these objects behind the scenes as you map source tables to destination tables. To orchestrate T-SQL loads, you can use Azure Data Factory, SSIS, or Azure functions.
* <a href="https://docs.microsoft.com/en-us/sql/integration-services/load-data-to-sql-data-warehouse?view=sql-server-ver16&preserve-view=true&viewFallbackFrom=azure-sqldw-latest">PolyBase with SSIS</a> works well when your source data is in SQL Server. SSIS defines the source to destination table mappings, and also orchestrates the load. If you already have SSIS packages, you can modify the packages to work with the new data warehouse destination.
* <a href="https://docs.microsoft.com/en-us/azure/data-factory/load-azure-sql-data-warehouse?tabs=data-factory">PolyBase with Azure Data Factory (ADF)</a> is another orchestration tool. It defines a pipeline and schedules jobs.
* <a href="https://docs.microsoft.com/en-us/azure/databricks/scenarios/databricks-extract-load-sql-data-warehouse?bc=%2Fazure%2Fsynapse-analytics%2Fbreadcrumb%2Ftoc.json&toc=%2Fazure%2Fsynapse-analytics%2Ftoc.json">PolyBase with Azure Databricks</a> transfers data from an Azure Synapse Analytics table to a Databricks dataframe and/or writes data from a Databricks dataframe to an Azure Synapse Analytics table using PolyBase.

**Non-PolyBase loading options**

If your data is not compatible with PolyBase, you can use bcp or the SQLBulkCopy API. BCP loads directly to dedicated SQL pool without going through Azure Blob storage, and is intended only for small loads. Note, the load performance of these options is slower than PolyBase.

5. Transform the data.

  While data is in the staging table, perform transformations that your workload requires. Then move the data into a production table.

6. Insert the data into production tables.

  The INSERT INTO ... SELECT statement moves the data from the staging table to the permanent table. As you design an ETL process, try running the process on a small test sample. Try extracting 1000 rows from the table to a file, move it to Azure, and then try loading it into a staging table.

  * <a href="https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/data-loading-best-practices">Best practices for loading data into a dedicated SQL pool in Azure Synapse Analytics</a>

**Best practices and considerations when using PolyBase**

Here are a few more things to consider when using PolyBase for SQL Data Warehouse loads:

* A single PolyBase load operation provides best performance.
* The load performance scales as you increase DWUs.
* PolyBase automatically parallelizes the data load process, so you don’t need to explicitly break the input data into multiple files and issue concurrent loads, unlike some traditional loading practices.  Each reader automatically read 512MB for each file for Azure Storage BLOB and 256MB on Azure Data Lake Storage.
* Multiple readers will not work against gzip files. Only a single reader is used per gzip compressed file since uncompressing the file in the buffer is single threaded. Alternatively, generate multiple gzip files.  The number of files should be greater than or equal to the total number of readers. 
* Multiple readers will work against compressed columnar/block format files (e.g. ORC, RC) since individual blocks are compressed independently.


#### 1.2 Send an email with an Azure Data Factory or Azure Synapse pipeline

To send a email notifications during or after the above execution of a pipeline that provides proactive alerting and reduces the need for reactive monitoring to discover issues.  This process shows how to configure email notifications from an Azure Data Factory or Azure Synapse pipeline. 

**Create the email workflow in your Logic App**

Create a Logic App workflow named `SendEmailFromPipeline`. Define the workflow trigger as `When finished loading by PolyBase`, and add an action of `Office 365 Outlook – Send an email (V2)`.

* <a href="https://docs.microsoft.com/en-us/azure/logic-apps/quickstart-create-first-logic-app-workflow">Quickstart: Create an integration workflow with multi-tenant Azure Logic Apps and the Azure portal</a>

**Create a pipeline to trigger your Logic App email workflow for Azure Blob Storage**

Setting up an alert is easy. At first, we need to define the alert condition (trigger or signal). An alert condition defines the metrics and the threshold that, when breached, the alert is to be triggered. 

1. In the Azure portal, locate and open the storage account. On the storage account page, search and open Alerts under the Monitoring section
2. On the Alerts page, click on New alert rule
3. On the Alerts | Create rule page, observe that the storage account is listed by default under the RESOURCE section. You can add multiple storage accounts in the same alert. Under the CONDITION section, click Add.
4. On the Configure signal logic page, select Used capacity under Signal name.
5. On the Configure signal logic page, under Alert logic, set Operator as Greater than, Aggregation type as Average, and configure the threshold to 5 MB. We need to provide the value in bytes.
6. Click Done to configure the trigger. The condition is added, and we'll be taken back to the Configure alerts rule page.
7. The next step is to add an action to perform when the alert condition is reached. On the Configure alerts rule page, under the ACTIONS GROUPS section, click Create.
8. On the Add action group page, provide the action group name, short name, and resource group. Under the Actions section, provide the action name and action type.
9. As we set Action Type as Email/SMS/Push/Voice, a new blade opens. In the Email/SMS/Push/Voice blade, specify the email name and click OK.
10. We are taken back to the Add action group page. On the Add action group page, click OK to save the action settings. We are then taken back to the Create rule page. The Email action is listed under the Action Groups section.
11. The next step is to define the alert rule name, description, and severity.
12. Click the Create alert rule button to create the alert.
13. The next step is to trigger the alert.
14. The triggered alerts are listed on the Alerts page.
15. An email is sent to the email ID specified in the email action group. 

We then need to define the action to be performed when the alert condition is reached. We can define more than one action for an alert. In our example, in addition to sending an email when the used capacity is more than 5 MB, we can configure Azure Automation to delete the old blobs/files so as to maintain the Azure storage capacity within 5 MB.

There are other signals such as transactions, Ingress, Egress, Availability, Success Server Latency, and Success E2E Latency on which alerts can be defined. Detailed information on monitoring Azure storage is available at https://docs.microsoft.com/en-us/azure/storage/common/storage-monitoring-diagnosing-troubleshooting.

