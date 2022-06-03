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


#### 1.2 Send an email with an Azure Data Factory or Azure Synapse pipeline

To send a email notifications during or after the above execution of a pipeline that provides proactive alerting and reduces the need for reactive monitoring to discover issues.  This process shows how to configure email notifications from an Azure Data Factory or Azure Synapse pipeline. 

**Create the email workflow in your Logic App**

Create a Logic App workflow named `SendEmailFromPipeline`. Define the workflow trigger as `When finished loading by PolyBase`, and add an action of `Office 365 Outlook – Send an email (V2)`.

**Create a pipeline to trigger your Logic App email workflow**

